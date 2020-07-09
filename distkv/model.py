"""
This module contains DistKV's basic data model.

TODO: message chains should be refactored to arrays: much lower overhead.
"""

from __future__ import annotations

import weakref
from range_set import RangeSet
from collections import defaultdict

from typing import List, Any

from .util import attrdict, NotGiven, Path
from .exceptions import ACLError
from anyio import create_queue

from logging import getLogger

logger = getLogger(__name__)


class NodeDataSkipped(Exception):
    def __init__(self, node):
        super().__init__()
        self.node = node

    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.node)


ConvNull = None  # imported later, if/when needed
NullACL = None  # imported later, if/when needed


class Node:
    """Represents one DistKV participant.
    """

    name: str = None
    tick: int = None
    _present: RangeSet = None  # I have these as valid data. Superset of ``._deleted``.
    _deleted: RangeSet = None  # I have these as no-longer-valid data
    _reported: RangeSet = None  # somebody else reported these missing data for this node
    _superseded: RangeSet = None  # I know these once existed, but no more.
    entries: dict = None
    tock: int = 0  # tock when node was last observed

    def __new__(cls, name, tick=None, cache=None, create=True):
        try:
            self = cache[name]
        except KeyError:
            if not create:
                raise
            self = object.__new__(cls)
            self.name = name
            self.tick = tick
            self._present = RangeSet()
            self._deleted = RangeSet()
            self._reported = RangeSet()
            self._superseded = RangeSet()
            self.entries = {}
            cache[name] = self
        else:
            if tick is not None:
                if self.tick is None or self.tick < tick:
                    self.tick = tick
        return self

    # pylint: disable=unused-argument
    def __init__(self, name, tick=None, cache=None, create=True):
        return

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, Node):
            other = other.name
        return self.name == other

    def __getitem__(self, item):
        return self.entries[item]

    def get(self, item, default=None):
        return self.entries.get(item, default)

    def enumerate(self, n: int = 0, current: bool = False):
        """
        Return a list of valid keys for that node.

        Used to find data from no-longer-used nodes so they can be deleted.
        """
        for k, v in self.entries.items():
            if current and v.chain is not None and v.chain.node is not self:
                continue
            yield k
            if n:
                n -= 1
                if not n:
                    return

    def __contains__(self, item):
        return item in self.entries

    def __repr__(self):
        return "<%s: %s @%s>" % (self.__class__.__name__, self.name, self.tick)

    def seen(self, tick, entry=None, local=False):
        """An event with this tick was in the entry's chain.

        Args:
          ``tick``: The event affecting the given entry.
          ``entry``: The entry affected by this event.
          ``local``: The message was not broadcast, thus do not assume that
                     other nodes saw this.

        """
        if not local:
            self._reported.discard(tick)
        if entry is not None:
            self._present.add(tick)
            self.entries[tick] = entry

            # this might happen when loading old state.
            if tick in self._superseded:
                self._superseded.discard(tick)
                logger.info("%s was marked as superseded", entry)

    def is_deleted(self, tick):
        """
        Check whether this tick has been marked as deleted.
        """
        return tick in self._deleted

    def mark_deleted(self, tick):
        """
        The data for this tick will be deleted.

        Args:
          tick: The event that caused the deletion.

        Returns: the entry, if still present
        """
        self._deleted.add(tick)
        return self.entries.get(tick, None)

    def clear_deleted(self, tick):
        """
        The data for this tick are definitely gone (deleted).
        """
        self._present.discard(tick)
        self._deleted.discard(tick)
        self._reported.discard(tick)
        self._superseded.add(tick)

        e = self.entries.pop(tick, None)
        if e is not None:
            e.purge_deleted()

    def purge_deleted(self, r: RangeSet):
        """
        All entries in this rangeset are deleted.

        This is a shortcut for calling :meth:`clear_deleted` on each item.
        """
        self._present -= r
        self._deleted -= r
        self._reported -= r
        self._superseded += r

        # Mark that these as really gone.
        for a, b in r:
            for t in range(a, b):
                e = self.entries.pop(t, None)
                if e is not None:
                    e.purge_deleted()

    def supersede(self, tick):
        """
        The event with this tick is no longer in the referred entry's chain.
        This happens when an entry is updated.

        Args:
          ``tick``: The event that once affected the given entry.
        """
        self._present.discard(tick)
        self._superseded.add(tick)
        self.entries.pop(tick, None)

    def report_superseded(self, r: RangeSet, local=False):
        """
        Some node said that these entries may have been superseded.

        Args:
          ``range``: The RangeSet thus marked.
          ``local``: The message was not broadcast, thus do not assume that
                     other nodes saw this.
        """
        coll = r & self._present
        r -= coll
        self._superseded += r
        if not local:
            self._reported -= r

        # Are any of these present?
        for a, b in coll:
            for t in range(a, b):
                e = self.entries[t]
                if e.chain.node is self and e.chain.tick == t:
                    logger.info("%s present but marked as superseded", e)

    def report_missing(self, r: RangeSet):
        """
        Some node doesn't know about these ticks.

        We may need to broadcast either their content,
        or the fact that these ticks have been superseded.
        """
        self._reported += r

    def report_deleted(self, r: RangeSet, server):
        """
        This range has been reported as deleted.

        Args:
          range (RangeSet): the range that's gone.
          add (dict): store additional vanished items. Nodename -> RangeSet
        """
        # Ignore those which we know have been deleted
        n = r - self._deleted

        for a, b in n:
            for t in range(a, b):
                entry = self.mark_deleted(t)
                if entry is None:
                    continue
                chain = entry.mark_deleted(server)
                if chain is None:
                    continue
                for node, tick in chain:
                    server.mark_deleted(node, tick)

        # Mark as deleted. The system will flush them later.
        self._deleted += r
        self._reported -= r
        self._superseded += r

        r &= self._present
        for a, b in r:
            for t in range(a, b):
                e = self.entries.get(t, None)
                if e is not None:
                    if e.data is not NotGiven:
                        logger.info("%s present but marked as deleted", e)
                    e.purge_deleted()

    @property
    def local_present(self):
        """Values I know about"""
        return self._present

    @property
    def local_superseded(self):
        """Values I knew about"""
        return self._superseded

    @property
    def local_deleted(self):
        """Values I know to have vanished"""
        return self._deleted

    @property
    def local_missing(self):
        """Values I have not seen, the inverse of :meth:`local_present`
        plus :meth:`local_superseded`"""
        assert self.tick
        r = RangeSet(((1, self.tick + 1),))
        r -= self._present
        r -= self._superseded
        return r

    @property
    def remote_missing(self):
        """Values from this node which somebody else has not seen"""
        return self._reported

    def kill_this_node(self, cache=None):
        """
        Remove this node from the system.
        No chain's first link may point to this node.
        """
        for e in self.entries.values():
            if e.chain.node is self:
                raise RuntimeError(f"Still main node at {e!r}")
            c = e.chain.filter(self)
            if c is None:
                raise RuntimeError(f"Empty chain after filter for {self.name} at {e!r}")
            e.chain = c

        if cache is not None:
            cache.pop(self.name, None)


class NodeSet(defaultdict):
    """
    Represents a dict (nodename > RangeSet).
    """

    def __init__(self, encoded=None, cache=None):
        super().__init__(RangeSet)
        if encoded is not None:
            assert cache is not None
            for n, v in encoded.items():
                n = Node(n, cache=cache)
                r = RangeSet()
                r.__setstate__(v)
                self[n] = r

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__, dict.__repr__(self))

    def __bool__(self):
        for v in self.values():
            if v:
                return True
        return False

    def serialize(self):
        d = dict()
        for k, v in self.items():
            assert not hasattr(k, "name")
            d[k] = v.__getstate__()
        return d

    @classmethod
    def deserialize(cls, state):
        self = cls()
        for k, v in state.items():
            r = RangeSet()
            r.__setstate__(v)
            self[k] = v
        return self

    def copy(self):
        res = type(self)()
        for k, v in self.items():
            if v:
                res[k] = v.copy()
        return res

    def add(self, node, tick):
        assert not hasattr(node, "name")
        self[node].add(tick)

    def __isub__(self, other):
        for k, v in other.items():
            r = self.get(k, None)
            if r is None:
                continue
            r -= v


class NodeEvent:
    """Represents any event originating at a node.

    Args:
      ``node``: The node thus affected
      ``tick``: Counter, timestamp, whatever
      ``prev``: The previous event, if any

    """

    def __init__(self, node: Node, tick: int = None, prev: "NodeEvent" = None):
        self.node = node
        if tick is None:
            tick = node.tick
        self.tick = tick
        if tick is not None and tick > 0:
            node.seen(tick)
        self.prev = None
        if prev is not None:
            self.prev = prev

    def __len__(self):
        """Length of this chain"""
        if self.prev is None:
            return 1
        return 1 + len(self.prev)

    def __repr__(self):
        return "<%s:%s @%s %s>" % (
            self.__class__.__name__,
            self.node,
            "-" if self.tick is None else self.tick,
            len(self),
        )

    def __iter__(self):
        c = self
        while c is not None:
            yield c.node, c.tick
            c = c.prev

    def __eq__(self, other):
        if other is None:
            return False
        return self.node == other.node and self.tick == other.tick

    def equals(self, other):
        """Check whether these chains are equal. Used for ping comparisons.

        The last two items may be missing from either chain.
        """
        if other is None:
            return self.prev is None or len(self.prev) <= 1
        if self != other:
            return False
        if self.prev is None:
            return other.prev is None or len(other.prev) <= 2
        elif other.prev is None:
            return self.prev is None or len(self.prev) <= 2
        else:
            return self.prev.equals(other.prev)

    def __lt__(self, other):
        """Check whether this node precedes ``other``, i.e. "other" is an
        event that is based on this.
        """
        if other is None:
            return False
        if self == other:
            return False
        while self.node != other.node:
            other = other.prev
            if other is None:
                return False
        return self.tick < other.tick

    def __gt__(self, other):
        """Check whether this node succedes ``other``, i.e. this event is
        based on it.
        """
        if other is None:
            return True
        if self == other:
            return False
        while self.node != other.node:
            self = self.prev  # pylint: disable=self-cls-assignment
            if self is None:
                return False
        return self.tick > other.tick

    def __lte__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __gte__(self, other):
        return self.__eq__(other) or self.__gt__(other)

    def __contains__(self, node):
        return self.find(node) is not None

    def find(self, node):
        """Return the position of a node in this chain.
        Zero if the first entry matches.

        Returns ``None`` if not present.
        """
        res = 0
        while self is not None:
            if self.node == node:
                return res
            res += 1
            self = self.prev  # pylint: disable=self-cls-assignment
        return None

    def filter(self, node, server=None):
        """Return an event chain without the given node.

        If the node is not in the chain, the result is *not* a copy.
        """
        if self.node == node:
            if server is not None:
                server.drop_old_event(self)
            return self.prev
            # Invariant: a node can only be in the chain once
            # Thus we can stop filtering after we encounter it.
        if self.prev is None:
            return self
        prev = self.prev.filter(node, server=server)
        if prev is self.prev:
            # No change, so return unmodified
            return self
        return NodeEvent(node=self.node, tick=self.tick, prev=prev)

    def serialize(self, nchain=-1) -> dict:
        if not nchain:
            raise RuntimeError("A chopped-off NodeEvent must not be sent")
        res = attrdict(node=self.node.name)
        if self.tick is not None:
            res.tick = self.tick
        if self.prev is None:
            res.prev = None
        elif nchain != 1:
            res.prev = self.prev.serialize(nchain - 1)
        return res

    @classmethod
    def deserialize(cls, msg, cache):
        if msg is None:
            return None
        msg = msg.get("chain", msg)
        tick = msg.get("tick", None)
        if "node" not in msg:
            assert "prev" not in msg
            assert tick is None
            return None
        self = cls(node=Node(msg["node"], tick=tick, cache=cache), tick=tick)
        if "prev" in msg:
            self.prev = cls.deserialize(msg["prev"], cache=cache)
        return self

    def attach(self, prev: "NodeEvent" = None, server=None):
        """Copy this node, if necessary, and attach a filtered `prev` chain to it"""
        if prev is not None:
            prev = prev.filter(self.node, server=server)
        if self.prev is not None or prev is not None:
            self = NodeEvent(  # pylint: disable=self-cls-assignment
                node=self.node, tick=self.tick, prev=prev
            )
        return self


class UpdateEvent:
    """Represents an event which updates something.
    """

    def __init__(self, event: NodeEvent, entry: "Entry", new_value, old_value=NotGiven, tock=None):
        self.event = event
        self.entry = entry
        self.new_value = new_value
        if old_value is not NotGiven:
            self.old_value = old_value
        if new_value is NotGiven:
            event.node.mark_deleted(event.tick)
        self.tock = tock

    def __repr__(self):
        if self.entry.chain == self.event:
            res = ""
        else:
            res = repr(self.event) + ": "
        return "<%s:%s%s: %s→%s>" % (
            self.__class__.__name__,
            res,
            repr(self.entry),
            "-"
            if not hasattr(self, "old_value")
            else ""
            if self.new_value == self.entry.data
            else repr(self.old_value),
            repr(self.new_value),
        )

    def serialize(self, chop_path=0, nchain=-1, with_old=False, conv=None):
        if conv is None:
            # pylint: disable=redefined-outer-name
            global ConvNull
            if ConvNull is None:
                from .types import ConvNull
            conv = ConvNull
        res = self.event.serialize(nchain=nchain)
        res.path = self.entry.path[chop_path:]
        if with_old:
            if self.old_value is not NotGiven:
                res.old_value = conv.enc_value(self.old_value, entry=self.entry)
            if self.new_value is not NotGiven:
                res.new_value = conv.enc_value(self.new_value, entry=self.entry)
        elif self.new_value is not NotGiven:
            res.value = conv.enc_value(self.new_value, entry=self.entry)
        res.tock = self.entry.tock
        return res

    @classmethod
    def deserialize(cls, root, msg, cache, nulls_ok=False, conv=None):
        if conv is None:
            # pylint: disable=redefined-outer-name
            global ConvNull
            if ConvNull is None:
                from .types import ConvNull
            conv = ConvNull
        entry = root.follow(msg.path, create=True, nulls_ok=nulls_ok)
        event = NodeEvent.deserialize(msg, cache=cache)
        old_value = NotGiven
        if "value" in msg:
            value = conv.dec_value(msg.value, entry=entry)
        elif "new_value" in msg:
            value = conv.dec_value(msg.new_value, entry=entry)
            old_value = conv.dec_value(msg.old_value, entry=entry)
        else:
            value = NotGiven

        return cls(event, entry, value, old_value=old_value, tock=msg.tock)


class Entry:
    """This class represents one key/value pair
    """

    _parent: "Entry" = None
    name: str = None
    _path: List[str] = None
    _root: "Entry" = None
    chain: NodeEvent = None
    SUBTYPE = None
    SUBTYPES = {}
    _data: Any = NotGiven

    monitors = None

    def __init__(self, name: str, parent: "Entry", tock=None):
        self.name = name
        self._sub = {}
        self.monitors = set()
        self.tock = tock

        if parent is not None:
            parent._add_subnode(self)
            self._parent = weakref.ref(parent)

    def _add_subnode(self, child: "Entry"):
        self._sub[child.name] = child

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if other is None:
            return False
        if isinstance(other, Entry):
            other = other.name
        return self.name == other

    def chain_links(self):
        c = self.chain
        if c is not None:
            yield from iter(c)

    def keys(self):
        return self._sub.keys()

    def values(self):
        return self._sub.values()

    def items(self):
        return self._sub.items()

    def __len__(self):
        return len(self._sub)

    def __bool__(self):
        return self._data is not None or len(self._sub) > 0

    def __contains__(self, key):
        return key in self._sub

    @property
    def path(self):
        if self._path is None:
            parent = self.parent
            if parent is None:
                self._path = Path()
            else:
                self._path = parent.path + [self.name]
        return self._path

    def follow_acl(self, path, *, create=True, nulls_ok=False, acl=None, acl_key=None):
        """Follow this path.

        If ``create`` is True (default), unknown nodes are silently created.
        Otherwise they cause a `KeyError`. If ``None``, assume
        ``create=True`` but only check the ACLs.

        If ``nulls_ok`` is False (default), `None` is not allowed as a path
        element. If 2, it is allowed anywhere; if True, only as the first
        element.

        If ``acl`` is not ``None``, then ``acl_key`` is the ACL letter to
        check for. ``acl`` must be an :class:`~distkv.types.ACLFinder`
        created from the root of the ACL in question.

        The ACL key 'W' is special: it checks 'c' if the node is new, else
        'w'.

        Returns a (node, acl) tuple.
        """

        # KEEP IN SYNC with `follow`, below!
        if acl is None:
            global NullACL
            if NullACL is None:
                from .types import NullACL  # pylint: disable=redefined-outer-name
            acl = NullACL

        first = True
        for name in path:
            if name is None and not nulls_ok:
                raise ValueError("Null path element")
            if nulls_ok == 1:  # root only
                nulls_ok = False
            child = self._sub.get(name, None) if self is not None else None
            if child is None:
                if create is False:
                    raise KeyError(path)
                acl.check("n")
                if create is not None:
                    child = self.SUBTYPES.get(name, self.SUBTYPE)
                    if child is None:
                        raise ValueError(f"Cannot add {name} to {self}")
                    child = child(name, self, tock=self.tock)
            else:
                acl.check("x")
            try:
                acl = acl.step(name, new=first)
            except KeyError:
                raise ACLError(acl.result, name) from None
            first = False
            self = child  # pylint: disable=self-cls-assignment

        # If the caller doesn't know if the node exists, help them out.
        if acl_key == "W":
            acl_key = "w" if self is not None and self._data is not NotGiven else "c"
        acl.check(acl_key)
        return (self, acl)

    def follow(self, path, *, create=True, nulls_ok=False):
        """
        As :meth:`follow_acl`, but isn't interested in ACLs and only returns the node.
        """
        # KEEP IN SYNC with `follow_acl`, above!

        for name in path:
            if name is None and not nulls_ok:
                raise ValueError("Null path element")
            if nulls_ok == 1:  # root only
                nulls_ok = False
            child = self._sub.get(name, None) if self is not None else None
            if child is None:
                if create is False:
                    raise KeyError(path)
                if create is not None:
                    child = self.SUBTYPES.get(name, self.SUBTYPE)
                    if child is None:
                        raise ValueError(f"Cannot add {name} to {self}")
                    child = child(name, self, tock=self.tock)
            self = child  # pylint: disable=self-cls-assignment
        return self

    def __getitem__(self, name):
        return self._sub[name]

    @property
    def root(self):
        root = self._root
        if root is not None:
            root = root()
            if root is None:
                raise RuntimeError("Root node is gone")
            return root

        parent = self.parent
        if parent is None:
            return self
        root = parent.root
        self._root = weakref.ref(root)
        return root

    async def set(self, value):
        self._data = value

    @property
    def parent(self):
        parent = self._parent
        if parent is None:
            return None
        parent = parent()
        if parent is None:
            raise RuntimeError("Parent node is gone")
        return parent

    def __repr__(self):
        try:
            res = "<%s:%s" % (self.__class__.__name__, self.path)
            if self.chain is not None:
                res += "@%s" % (repr(self.chain),)
            if self.data is not None:
                res += " =%s" % (repr(self.data),)
            res += ">"
        except Exception as exc:
            res = "<%s:%s" % (self.__class__.__name__, str(exc))
        return res

    @property
    def data(self):
        return self._data

    def mark_deleted(self, server):
        """
        This entry has been deleted.

        Returns:
          the entry's chain.
        """
        c = self.chain
        for node, tick in self.chain_links():
            e = node.mark_deleted(tick)
            assert e is None or e is self
            server.mark_deleted(node, tick)
        self._data = NotGiven
        return c

    def purge_deleted(self):
        """
        Call :meth:`Node.clear_deleted` on each link in this entry's chain.
        """
        c, self.chain = self.chain, None
        if c is None:
            return
        for node, tick in c:
            node.clear_deleted(tick)
        self._chop()

    def _chop(self):
        """
        Remove a deleted entry (and possibly its parent).
        """
        logger.debug("CHOP %r", self)
        this, p = self, self._parent
        while p is not None:
            p = p()
            if p is None:
                return
            p._sub.pop(this.name, None)
            if p._sub:
                return
            this, p = p, p._parent

    async def set_data(self, event: NodeEvent, data: Any, server=None, tock=None):
        """This entry is updated by that event.

        Args:
          event: The :class:`NodeEvent` to base the update on.
          data (Any): whatever the node should contains. Use :any:`distkv.util.NotGiven`
            to delete.

        Returns:
          The :class:`UpdateEvent` that has been generated and applied.
        """
        event = event.attach(self.chain, server=server)
        evt = UpdateEvent(event, self, data, self._data, tock=tock)
        await self.apply(evt, server=server)
        return evt

    async def apply(self, evt: UpdateEvent, server=None, root=None, loading=False):
        """Apply this :cls`UpdateEvent` to me.

        Also, forward to watchers.
        """
        chk = None
        if root is not None and None in root:
            chk = root[None].get("match", None)

        if evt.event is None:
            raise RuntimeError("huh?")

        if evt.event == self.chain:
            assert self._data == evt.new_value, (
                "has:",
                self._data,
                "but should have:",
                evt.new_value,
            )
            return

        if hasattr(evt, "new_value"):
            evt_val = evt.new_value
        else:
            evt_val = evt.value

        if self.chain > evt.event:  # already superseded
            logger.warning("*** superseded ***")
            logger.warning("Node: %s", self.path)
            logger.warning("Current: %s :%s: %r", self.chain, self.tock, self._data)
            logger.warning("New: %s :%s: %r", evt.event, evt.tock, evt_val)
            return

        if self._data is not NotGiven:
            if not (self.chain < evt.event):
                if not loading:
                    logger.warning("*** inconsistency ***")
                    logger.warning("Node: %s", self.path)
                    logger.warning("Current: %s :%s: %r", self.chain, self.tock, self._data)
                    logger.warning("New: %s :%s: %r", evt.event, evt.tock, evt_val)
                if evt.tock < self.tock:
                    if not loading:
                        logger.warning("New value ignored")
                    # also mark the new event's chain as superseded
                    server.drop_old_event(self.chain, evt.event)
                    return
                if not loading:
                    logger.warning("New value used")

        if chk is not None and evt_val is not NotGiven:
            chk.check_value(evt_val, self)
        if not hasattr(evt, "old_value"):
            evt.old_value = self._data
        await self.set(evt_val)
        self.tock = evt.tock

        server.drop_old_event(evt.event, self.chain)
        self.chain = evt.event

        if evt_val is NotGiven:
            self.mark_deleted(server)

        for n, t in self.chain_links():
            n.seen(t, self)
        await self.updated(evt)

    async def walk(self, proc, acl=None, max_depth=-1, min_depth=0, _depth=0, full=False):
        """
        Call coroutine ``proc`` on this node and all its children).

        If `acl` (must be an ACLStepper) is given, `proc` is called with
        the acl as second argument.

        If `proc` raises `StopAsyncIteration`, chop this subtree.
        """
        if min_depth <= _depth:
            try:
                if acl is not None:
                    await proc(self, acl)
                else:
                    await proc(self)
            except StopAsyncIteration:
                return
        if max_depth == _depth:
            return
        _depth += 1
        for k, v in list(self._sub.items()):
            if k is None and not full:
                continue
            a = acl.step(k) if acl is not None else None
            await v.walk(proc, acl=a, max_depth=max_depth, min_depth=min_depth, _depth=_depth)

    def serialize(self, chop_path=0, nchain=2, conv=None):
        """Serialize this entry for msgpack.

        Args:
          ``chop_path``: If <0, do not return the entry's path.
                         Otherwise, do, but remove the first N entries.
          ``nchain``: how many change events to include.
        """
        if conv is None:
            global ConvNull
            if ConvNull is None:
                from .types import ConvNull  # pylint: disable=redefined-outer-name
            conv = ConvNull
        res = attrdict()
        if self._data is not NotGiven:
            res.value = conv.enc_value(self._data, entry=self)
        if self.chain is not None and nchain != 0:
            res.chain = self.chain.serialize(nchain=nchain)
        res.tock = self.tock
        if chop_path >= 0:
            path = self.path
            if chop_path > 0:
                path = path[chop_path:]
            res.path = path
        return res

    async def updated(self, event: UpdateEvent):
        """Send an event to this node (and all its parents)'s watchers."""
        node = self
        while True:
            bad = set()
            for q in list(node.monitors):
                if q._distkv__free is None or q._distkv__free > 1:
                    if q._distkv__free is not None:
                        q._distkv__free -= 1
                    await q.put(event)
                else:
                    bad.add(q)
            for q in bad:
                try:
                    if q._distkv__free > 0:
                        await q.put(None)
                    node.monitors.remove(q)
                except KeyError:
                    pass
                else:
                    pass
                    # await q.aclose()
            node = node.parent
            if node is None:
                break

    _counter = 0

    @property
    def counter(self):
        self._counter += 1
        return self._counter


Entry.SUBTYPE = Entry


class Watcher:
    """
    This helper class is used as an async context manager plus async
    iterator. It reports all updates to an entry (or its children).

    If a watcher terminates, sending to its channel has blocked.
    The receiver needs to take appropriate re-syncing action.
    """

    root: Entry = None
    q = None
    q_len = 100

    def __init__(self, root: Entry, full: bool = False, q_len: int = None):
        self.root = root
        self.full = full
        if q_len is not None:
            self.q_len = q_len

    async def __aenter__(self):
        if self.q is not None:
            raise RuntimeError("You cannot enter this context more than once")
        self.q = create_queue(self.q_len)
        self.q._distkv__free = self.q_len or None
        self.root.monitors.add(self.q)
        return self

    async def __aexit__(self, *tb):
        self.root.monitors.remove(self.q)
        self.q = None

    def __aiter__(self):
        if self.q is None:
            raise RuntimeError("You need to enclose this with 'async with'")
        return self

    async def __anext__(self):
        if self.q is None:
            raise RuntimeError("Aborted. Queue filled?")
        while True:
            res = await self.q.get()
            if self.q._distkv__free is not None:
                self.q._distkv__free += 1
            if res is None:
                raise RuntimeError("Aborted. Queue filled?")
            if len(res.entry.path) and res.entry.path[0] is None and not self.full:
                continue
            return res
