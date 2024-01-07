"""
Object interface to moat.kv data

"""

import heapq
import weakref
from collections.abc import Mapping
from functools import partial

import anyio
from asyncscope import scope

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from moat.util import NoLock, NotGiven, Path, PathLongener, combine_dict, yload

__all__ = ["ClientEntry", "AttrClientEntry", "ClientRoot"]


class NamedRoot:
    """
    This is a mix-on class for the root of a subhierarchy that caches named
    sub-entries.

    Named children should call `_add_name` on this entry.
    """

    def __init__(self, *a, **k):
        self.__named = {}
        super().__init__(*a, **k)

    def by_name(self, name):
        if name is None:
            return None
        if not isinstance(name, str):
            raise ValueError("No string: " + repr(name))
        return self.__named.get(name)

    def _add_name(self, obj):
        n = obj.name
        if n is None:
            return

        self.__named[n] = obj
        obj.reg_del(self, "_del__name", obj, n)

    def _del__name(self, obj, n):
        old = self.__named.pop(n)
        if old is None or old is obj:
            return
        # Oops, that has been superseded. Put it back.
        self.__named[n] = old


class ClientEntry:
    """A helper class that represents a node on the server, as returned by
    :meth:`Client.mirror`.
    """

    value = NotGiven
    chain = None

    def __init__(self, parent, name=None):
        self._init()
        self._path = parent._path + (name,)
        self._name = name
        self._parent = weakref.ref(parent)
        self._root = weakref.ref(parent.root)
        self.client = parent.client

    def _init(self):
        self._lock = anyio.Lock()  # for saving etc.
        self.chain = None
        self.value = NotGiven
        self._children = dict()

    @classmethod
    def child_type(cls, name):  # pylint: disable=unused-argument
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        return cls

    def value_or(self, default, typ=None):
        """
        Shortcut to coerce the value to some type
        """
        val = self.value
        if val is NotGiven:
            return default
        if typ is not None and not isinstance(val, typ):
            return default
        return val

    def val(self, *attr):
        """
        Shortcut to get an attribute value
        """
        return self.val_d(NotGiven, *attr)

    def val_d(self, default, *attr):
        """
        Shortcut to get an attribute value, or a default
        """
        val = self.value
        if val is NotGiven:
            if default is NotGiven:
                raise ValueError("no value set")
            return default
        for a in attr:
            try:
                val = val[a]
            except KeyError:
                if default is NotGiven:
                    raise
                return default
        return val

    def find_cfg(self, *k, default=NotGiven):
        """
        Convenience method to get a config value

        It is retrieved first from this node's value, then from the parent.
        """
        val = self.value_or({}, Mapping)
        try:
            for kk in k:
                try:
                    val = val[kk]
                except TypeError:
                    raise TypeError(self.value, k, val, kk)
            return val
        except KeyError:
            return self.parent.find_cfg(*k, default=default)

    @property
    def parent(self):
        return self._parent()

    @property
    def root(self):
        return self._root()

    @property
    def subpath(self):
        """Return the path to this entry, starting with its :class:`ClientRoot` base."""
        return self._path[len(self.root._path) :]  # noqa: E203

    @property
    def all_children(self):
        """Iterate all child nodes with data.

        You can send ``True`` to the iterator if you want to skip a subtree.
        """
        for k in self:
            if k.value is not NotGiven:
                res = yield k
                if res is True:
                    continue
            yield from k.all_children

    def allocate(self, name: str, exists: bool = False):
        """
        Create the child named "name". It is created (locally) if it doesn't exist.

        Arguments:
          name: The child node's name.
          exists: return the existing value? otherwise error

        If this returns ``None``, the subtree shall not be tracked.

        """
        c = self._children.get(name, None)
        if c is not None:
            if exists:
                return c
            raise RuntimeError("Duplicate child", self, name, c)
        c = self.child_type(name)
        if c is None:
            raise KeyError(name)
        self._children[name] = c = c(self, name)
        return c

    def follow(self, path, *, create=None, empty_ok=False):
        """Look up a sub-entry.

        Arguments:
          path (Path): the path elements to follow.
          create (bool): Create the entries. Default ``False``.
            Otherwise return ``None`` if not found.

        The path may not be empty. It also must not be a string,
        because that indicates that you called ``.follow(*path)`` instead of
        ``.follow(path)``.
        """
        if isinstance(path, str):
            raise RuntimeError("You seem to have used '*path' instead of 'path'.")
        if not empty_ok and not len(path):
            raise RuntimeError("Empty path")

        node = self
        for n, elem in enumerate(path, start=1):
            next_node = node.get(elem)
            if next_node is None:
                if create is False:
                    return None
                next_node = node.allocate(elem)
            elif create and n == len(path) and next_node.value is not NotGiven:
                raise RuntimeError("Duplicate child", self, path, n)

            node = next_node

        return node

    def __getitem__(self, name):
        return self._children[name]

    def by_name(self, name):
        """
        Lookup by a human-readable name?
        """
        return self._children[name]

    def __delitem__(self, name):
        del self._children[name]

    def get(self, name):
        return self._children.get(name, None)

    def __iter__(self):
        """Iterating an entry returns its children."""
        return iter(list(self._children.values()))

    def __bool__(self):
        return self.value is not NotGiven or bool(self._children)

    def __len__(self):
        return len(self._children)

    def __contains__(self, k):
        if isinstance(k, type(self)):
            k = k._name
        return k in self._children

    async def update(self, value, _locked=False, wait=False):
        """Update (or simply set) this node's value.

        This is a coroutine.
        """
        async with NoLock if _locked else self._lock:
            r = await self.root.client.set(
                self._path, chain=self.chain, value=value, nchain=3, idem=True
            )
            if wait:
                await self.root.wait_chain(r.chain)
            self.value = value
            self.chain = r.chain
            return r

    async def delete(
        self, _locked=False, nchain=0, chain=True, wait=False, recursive=False
    ):
        """Delete this node's value.

        This is a coroutine.
        """
        async with NoLock if _locked else self._lock:
            r = await self.root.client.delete(
                self._path,
                nchain=nchain,
                recursive=recursive,
                **({"chain": self.chain} if chain else {}),
            )
            if wait:
                await self.root.wait_chain(r.chain)
            self.chain = None
            return r

    async def set_value(self, value):
        """Callback to set the value when data has arrived.

        This method is strictly for overriding.
        Don't call me, I'll call you.

        This is a coroutine, for ease of integration.
        """
        self.value = value

    async def seen_value(self):
        """The current value was seen (again).

        Useful for syncing.

        The default action is to do nothing.
        """
        pass

    def mark_inconsistent(self, r):
        """There has been an inconsistent update.

        This call will immediately be followed by a call to
        :meth:`set_value`, thus it is not async.

        The default action is to do nothing.
        """
        pass


def _node_gt(self, other):
    if other is None and self is not None:
        return True
    if self is None or self == other:
        return False
    while self["node"] != other["node"]:
        self = self["prev"]
        if self is None:
            return False
    return self["tick"] >= other["tick"]


class AttrClientEntry(ClientEntry):
    """A ClientEntry which expects a dict as value and sets (some of) the clients'
    attributes appropriately.

    Set the classvar ``ATTRS`` to a list of the attrs you want saved. Note
    that these are not inherited: when you subclass, copy and extend the
    ``ATTRS`` of your superclass.

    If the entry is deleted (value set to ``None``, the attributes listed in
    ``ATTRS`` will be deleted too, or revert to the class values.
    """

    ATTRS = ()

    async def update(self, value, **kw):  # pylint: disable=arguments-differ
        raise RuntimeError("Nope. Set attributes and call '.save()'.")

    async def set_value(self, value):
        """Callback to set the value when data has arrived.

        This method sets the actual attributes.

        This method is strictly for overriding.
        Don't call me, I'll call you.
        """
        await super().set_value(value)
        for k in self.ATTRS:
            if value is not NotGiven and k in value:
                setattr(self, k, value[k])
            else:
                try:
                    delattr(self, k)
                except AttributeError:
                    pass

    def get_value(self, skip_none=False, skip_empty=False):
        """
        Extract value from attrs
        """
        res = {}
        for attr in type(self).ATTRS:
            try:
                v = getattr(self, attr)
            except AttributeError:
                pass
            else:
                if v is NotGiven:
                    continue
                if skip_none and v is None:
                    continue
                if skip_empty and v == ():
                    continue
                res[attr] = v
        return res

    async def save(self, wait=False):
        """
        Save myself to storage, by copying ATTRS to a new value.
        """
        async with self._lock:
            r = await super().update(value=self.get_value(), _locked=True)
            if wait:
                await self.root.wait_chain(r.chain)
            return r


class MirrorRoot(ClientEntry):
    """
    This class represents the root of a subsystem's storage, used for
    object-agnostic data mirroring.

    Used internally.
    """

    _tg = None

    CFG = None  # You need to override this with a dict(prefix=('where','ever'))

    def __init__(self, client, path, *, need_wait=False, cfg=None, require_client=True):
        # pylint: disable=super-init-not-called
        self._init()
        self.client = client
        self._path = path
        self._need_wait = need_wait
        self._loaded = anyio.Event()
        self._require_client = require_client

        if cfg is None:
            cfg = {}
        self._cfg = cfg
        self._name = self.client.name

        if need_wait:
            self._waiters = dict()
            self._seen = dict()

    @classmethod
    async def as_handler(
        cls, client, cfg=None, key="prefix", subpath=(), name=None, **kw
    ):
        """Return an (or "the") instance of this class.

        The handler is created if it doesn't exist.

        Instances are distinguished by a key (from config), which
        must contain their path, and an optional subpath.
        """
        d = []
        if cfg is not None:
            d.append(cfg)

        defcfg = client._cfg.get(cls.CFG)
        if not defcfg:
            # seems we didn't load the class' default config yet.
            import inspect
            from pathlib import Path as _Path

            md = inspect.getmodule(cls)
            try:
                f = (_Path(md.__file__).parent / "_config.yaml").open("r")
            except EnvironmentError:
                pass
            else:
                with f:
                    defcfg = yload(f, attr=True).get("kv",{}).get(cls.CFG)
        if cfg:
            if defcfg:
                cfg = combine_dict(cfg, defcfg)
        else:
            if not defcfg:
                raise RuntimeError("no config for " + repr(cls))
            cfg = defcfg

        if name is None:
            # if key != "prefix":
            #     subpath = Path(key) + subpath
            name = str(Path("_moat.kv", client.name, cls.CFG, *subpath))

        def make():
            return client.mirror(
                cfg[key] + subpath, root_type=cls, need_wait=True, cfg=cfg, **kw
            )

        return await client.unique_helper(name, factory=make)

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is :class:`ClientEntry`.

        This may return ``None``. In that case the subtree with this name
        shall not be tracked further.
        """
        return ClientEntry

    @property
    def root(self):
        """Returns this instance."""
        return self

    def find_cfg(self, *k, default=NotGiven):
        """
        Convenience method to get a config value.

        It is retrieved first from this node's value, then from the configuration read via CFG.
        """
        val = self.value_or({}, Mapping)
        try:
            for kk in k:
                val = val[kk]
            return val
        except KeyError:
            try:
                val = self._cfg
                for kk in k:
                    val = val[kk]
                return val
            except KeyError:
                if default is NotGiven:
                    raise
                return default

    async def run_starting(self):
        """Hook for 'about to start reading'"""
        pass

    async def running(self):
        """Hook for 'done reading current state'"""
        self._loaded.set()

    @asynccontextmanager
    async def run(self):
        """A coroutine that fetches, and continually updates, a subtree."""
        if self._require_client:
            scope.requires(self.client.scope)

        async with anyio.create_task_group() as tg:
            self._tg = tg

            async def monitor(*, task_status):
                pl = PathLongener(())
                await self.run_starting()
                async with self.client._stream(
                    "watch", nchain=3, path=self._path, fetch=True
                ) as w:
                    async for r in w:
                        if "path" not in r:
                            if r.get("state", "") == "uptodate":
                                await self.running()
                            task_status.started()
                            continue
                        pl(r)
                        val = r.get("value", NotGiven)
                        entry = self.follow(r.path, create=None, empty_ok=True)
                        if entry is not None:
                            # Test for consistency
                            try:
                                if entry.chain == r.chain:
                                    # entry.update() has set this
                                    await entry.seen_value()
                                elif _node_gt(entry.chain, r.chain):
                                    # stale data
                                    pass
                                elif not _node_gt(r.chain, entry.chain):
                                    entry.mark_inconsistent(r)
                            except AttributeError:
                                pass

                            # update entry
                            entry.chain = (
                                None if val is NotGiven else r.get("chain", None)
                            )
                            await entry.set_value(val)

                            if val is NotGiven and not entry:
                                # the entry has no value and no children,
                                # so we delete it (and possibly its
                                # parents) from our tree.
                                n = list(entry.subpath)
                                while n:
                                    # no-op except for class-specific side effects
                                    # like setting an event
                                    await entry.set_value(NotGiven)

                                    entry = entry.parent
                                    del entry[n.pop()]
                                    if entry:
                                        break

                        if not self._need_wait or "chain" not in r:
                            continue
                        c = r.chain
                        while c is not None:
                            if self._seen.get(c.node, 0) < c.tick:
                                self._seen[c.node] = c.tick
                            try:
                                w = self._waiters[c.node]
                            except KeyError:
                                pass
                            else:
                                while w and w[0][0] <= c.tick:
                                    heapq.heappop(w)[1].set()
                            c = c.get("prev", None)

            await tg.start(monitor)
            try:
                yield self
            finally:
                with anyio.fail_after(2, shield=True):
                    tg.cancel_scope.cancel()
            pass  # end of 'run', closing taskgroup

    async def cancel(self):
        """Stop the monitor"""
        await self._tg.cancel_scope.cancel()

    async def wait_loaded(self):
        """Wait for the tree to be loaded completely."""
        await self._loaded.wait()

    async def wait_chain(self, chain):
        """Wait for a tree update containing this tick."""
        try:
            if chain.tick <= self._seen[chain.node]:
                return
        except KeyError:
            pass
        w = self._waiters.setdefault(chain.node, [])
        e = anyio.Event()
        heapq.heappush(w, (chain.tick, e))
        await e.wait()

    async def spawn(self, p, *a, **kw):
        p = partial(p, *a, **kw)
        self._tg.start_soon(p)


class ClientRoot(MirrorRoot):

    """
    This class represents the root of a subsystem's storage.

    To use this class, create a subclass that, at minimum, overrides
    ``CFG`` and ``child_type``. ``CFG`` must be a dict with at least a
    ``prefix`` tuple. You instantiate the entry using :meth:`as_handler`.

    """

    def __init__(self, *a, **kw):
        if self.CFG is None:
            raise TypeError(f"You need to override .CFG in {type(self).__name__}")
        super().__init__(*a, **kw)
