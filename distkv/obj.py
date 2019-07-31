"""
Object interface to distkv data

"""

import anyio
import outcome
import msgpack
import socket
import weakref
import heapq
import random
from functools import partial
import socket

try:
    from contextlib import asynccontextmanager, AsyncExitStack
except ImportError:
    from async_generator import asynccontextmanager
    from async_exit_stack import AsyncExitStack

from asyncserf.util import ValueEvent
from .util import (
    attrdict,
    gen_ssl,
    num2byte,
    byte2num,
    PathLongener,
    NoLock,
    NotGiven,
    combine_dict,
)
from .default import CFG
from .exceptions import (
    ClientAuthMethodError,
    ClientAuthRequiredError,
    ServerClosedError,
    ServerConnectionError,
    ServerError,
    CancelledError,
)

import logging

logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

__all__ = ["ClientEntry", "AttrClientEntry", "CLientRoot"]


class ClientEntry:
    """A helper class that represents a node on the server, as returned by
    :meth:`Client.mirror`.
    """

    def __init__(self, parent, name=None):
        self._children = dict()
        self._path = parent._path + (name,)
        self._name = name
        self.value = NotGiven
        self.chain = None
        self._parent = weakref.ref(parent)
        self._root = weakref.ref(parent.root)
        self.client = parent.client
        self._lock = anyio.create_lock()  # for saving etc.

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        return cls

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
                res = (yield k)
                if res is True:
                    continue
            yield from k.all_children

    def allocate(self, name):
        """
        Create the child named "name". It is created (locally) if it doesn't exist.

        Arguments:
          name (str): The child node's name.
        """
        if name in self._children:
            raise RuntimeError("Duplicate child",name,self)
        self._children[name] = c = self.child_type(name)(self, name)
        return c

    def __getitem__(self, name):
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

    async def update(self, value, _locked=False):
        """Update (or simply set) this node's value.

        This is a coroutine.
        """
        async with NoLock if _locked else self._lock:
            r = await self.root.client.set(
                *self._path, chain=self.chain, value=value, nchain=3
            )
            self.value = value
            self.chain = r.chain
            return r

    async def delete(self, _locked=False, nchain=0, chain=True):
        """Delete this node's value.

        This is a coroutine.
        """
        async with NoLock if _locked else self._lock:
            r = await self.root.client.delete(
                *self._path, nchain=nchain,
                **({"chain":self.chain} if chain else {}),
            )
            self.chain = None
            return r

    async def set_value(self, value=NotGiven):
        """Callback to set the value when data has arrived.

        This method is strictly for overriding.
        Don't call me, I'll call you.

        This is a coroutine, for ease of integration.
        """
        self.value = value

    async def seen_value(self):
        """Current value seen.

        Useful for syncing.
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

    async def update(self, val):
        raise RuntimeError("Nope. Set attributes and call '.save()'.")

    async def set_value(self, val=NotGiven):
        """Callback to set the value when data has arrived.

        This method sets the actual attributes.

        This method is strictly for overriding.
        Don't call me, I'll call you.
        """
        await super().set_value(val)
        for k in self.ATTRS:
            if val is not NotGiven and k in val:
                setattr(self, k, val[k])
            else:
                try:
                    delattr(self, k)
                except AttributeError:
                    pass

    def get_value(self, wait=False):
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
                if v is not NotGiven:
                    res[attr] = v
        return res


    async def save(self, wait=False):
        """
        Save myself to storage, by copying ATTRS to a new value.
        """
        res = {}
        async with self._lock:
            r = await super().update(value=self.get_value(), _locked=True)
            if wait:
                await self.root.wait_chain(r.chain)
            return r


class ClientRoot(ClientEntry):
    """This class represents the root of a subsystem's storage.

    To use this class, create a subclass that, at minimum, overrides
    ``CFG`` and ``child_type``. ``CFG`` must be a dict with at least a
    ``prefix`` tuple. You instantiate the entry using :meth:`as_handler`.

    """

    CFG = "You need to override this with a dict(prefix=('where','ever'))"

    def __init__(self, client, *path, need_wait=False, cfg=None):
        self.chain = None
        self._children = dict()
        self.client = client
        self._path = path
        self.value = None
        self._need_wait = need_wait
        self._loaded = anyio.create_event()
        if cfg is None:
            cfg = {}
        self._cfg = cfg
        self._name = self.client.name

        if need_wait:
            self._waiters = dict()
            self._seen = dict()

    @classmethod
    async def as_handler(cls, client, cfg=None, key="prefix", **kw):
        """Return a (or "the") instance of this class.

        The handler is created if it doesn't exist.

        Instances are distinguished by their prefix (from config).
        """
        d = []
        if cfg is not None:
            d.append(cfg)
        defcfg = CFG.get(cls.CFG, None)
        if cfg:
            if defcfg:
                cfg = combine_dict(cfg, defcfg)
        else:
            if not defcfg:
                raise RuntimeError("no config")
            cfg = defcfg

        def make():
            return client.mirror(
                *cfg[key], root_type=cls, need_wait=True, cfg=cfg, **kw
            )

        return await client.unique_helper(*cfg[key], factory=make)

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is :class:`ClientEntry`.
        """
        return ClientEntry

    @property
    def root(self):
        """Returns this instance."""
        return self

    def follow(self, *path, create=False, unsafe=False):
        """Look up a sub-entry.

        Arguments:
          *path (str): the path elements to follow.
          create (bool): Create the entries. Default ``False``.
            Otherwise return ``None`` if not found.
          unsafe (bool): Allow a single path element that's a tuple.
            This usually indicates a mistake by the caller. Defaults to
            ``False``. Please try not to need this.

        The path may not be empty. It also must not be a one-element list,
        because that indicates that you called ``.follow(path)`` instead of
        ``.follow(*path)``. To allow that, set ``unsafe``, though a better
        idea is to structure your data that this is not necessary.
        """
        if not unsafe and len(path) == 1 and isinstance(path[0], (list, tuple)):
            raise RuntimeError("You seem to have used 'path' instead of '*path'.")

        node = self
        for elem in path:
            next_node = node.get(elem)
            if next_node is None:
                if create:
                    next_node = node.allocate(elem)
                else:
                    return None

            node = next_node

        if not unsafe and node is self:
            raise RuntimeError("Empty path")
        return node

    async def run_starting(self):
        """Hook for 'about to start reading'"""
        pass

    async def running(self):
        """Hook for 'done reading current state'"""
        await self._loaded.set()

    @asynccontextmanager
    async def run(self):
        """A coroutine that fetches, and continually updates, a subtree.
        """
        async with anyio.create_task_group() as tg:
            self._tg = tg

            async def monitor():
                pl = PathLongener(())
                await self.run_starting()
                async with self.client._stream(
                    "watch", nchain=3, path=self._path, fetch=True
                ) as w:
                    async for r in w:
                        if "path" not in r:
                            if r.get("state", "") == "uptodate":
                                await self.running()
                            continue
                        pl(r)
                        val = r.get("value", NotGiven)
                        entry = self.follow(*r.path, create=(val is not NotGiven), unsafe=True)
                        if entry is not None:
                            # Test for consistency
                            try:
                                if entry.chain == r.chain:
                                    # entry.update() has set this
                                    await entry.seen_value()
                                    continue
                                if _node_gt(entry.chain, r.chain):
                                    # stale data
                                    continue
                                if not _node_gt(r.chain, entry.chain):
                                    entry.mark_inconsistent(r)
                            except AttributeError:
                                pass

                            # update entry
                            entry.prev_chain, entry.chain = entry.chain, (None if val is NotGiven else r.get("chain", None))
                            await entry.set_value(val)

                            if val is NotGiven and not entry:
                                # the entry has no value and no children,
                                # so we delete it (and possibly its
                                # parents) from our tree.
                                n = list(entry.subpath)
                                while n:
                                    # no-op except for class-specific side effects like setting an event
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
                                    await heapq.heappop(w)[1].set()
                            c = c.get("prev", None)

            await tg.spawn(monitor)
            try:
                yield self
            finally:
                await tg.cancel_scope.cancel()
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
        e = anyio.create_event()
        heapq.heappush(w, (chain.tick, e))
        await e.wait()

    def spawn(self, *a, **kw):
        return self._tg.spawn(*a, **kw)

