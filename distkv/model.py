# DistKV's data model

from __future__ import annotations

import weakref
import anyio
from range_set import RangeSet

from typing import Optional, List, Any

from .util import attrdict

from logging import getLogger
logger = getLogger(__name__)

class NodeDataSkipped(Exception):
    def __init__(self, node):
        self.node = node
    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.node)

class Node:
    """Represents one DistKV participant.
    """
    name: str = None
    tick: int = None
    _known: RangeSet = None # I have these as valid data
    _reported: RangeSet = None # somebody else reported these missing data for this node
    entries: dict = None

    def __new__(cls, name, tick=None, cache=None, create=True):
        try:
            self = cache[name]
        except KeyError:
            if not create:
                raise
            self = object.__new__(cls)
            self.name = name
            self.tick = tick
            self._known = RangeSet()
            self._reported = RangeSet()
            self.entries = {}
            cache[name] = self
        else:
            if tick is not None:
                if self.tick is None or self.tick < tick:
                    self.tick = tick
        return self

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

    def __contains__(self, item):
        return item in self.entries

    def __repr__(self):
        return "<%s: %s @%s>" % (self.__class__.__name__, self.name, self.tick)

    def seen(self, tick, entry=None, local=False):
        """An event with this tick is in the entry's chain.

        Args:
          ``tick``: The event affecting the given entry.
          ``entry``: The entry affected by this event.
          ``local``: The message was not broadcast, thus do not assume that
                     other nodes saw this.

        """
        self._known.add(tick)
        if not local:
            self._reported.discard(tick)
        if entry is not None:
            self.entries[tick] = entry

    def supersede(self, tick):
        """The event with this tick is no longer in the referred entry's chain.
        This happens when an entry is updated.
        
        Args:
          ``tick``: The event that once affected the given entry.
        """
        self.entries.pop(tick, None)

    def reported_known(self, range, local=False):
        """Some node said that these entries may have been superseded.
        
        Args:
          ``range``: The RangeSet thus marked.
          ``local``: The message was not broadcast, thus do not assume that
                     other nodes saw this.
        """
        self._known += range
        if not local:
            self._reported -= range

    def reported_missing(self, range):
        self._reported += range

    @property
    def local_known(self):
        """Values I have seen"""
        return self._known

    @property
    def local_missing(self):
        """Values I have not seen"""
        assert self.tick
        r = RangeSet(((1,self.tick+1),))
        r -= self._known
        return r

    @property
    def remote_missing(self):
        """Values from this node which somebody else has not seen"""
        return self._reported

class NodeEvent:
    """Represents any event originating at a node.

    Args:
      ``node``: The node thus affected
      ``tick``: Counter, timestamp, whatever
      ``prev``: The previous event, if any

    """
    def __init__(self, node:Node, tick:int = None, prev: NodeEvent = None, check_dup=True):
        self.node = node
        if tick is None:
            tick = node.tick
        self.tick = tick
        if check_dup and tick is not None and tick > 0:
            node.seen(tick)
        self.prev = None
        if prev is not None:
            self.prev = prev

    def __len__(self):
        if self.prev is None:
            return 1
        return 1+len(self.prev)

    def __repr__(self):
        return "<%s:%s @%s %s>" % (self.__class__.__name__, self.node, '-' if self.tick is None else self.tick, len(self))

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
        if other is None:
            return False
        if self == other:
            return False
        while self.node != other.node:
            other = other.prev
            if other is None:
                return False
        return self.tick <= other.tick

    def __gt__(self, other):
        if other is None:
            return True
        if self == other:
            return False
        while self.node != other.node:
            self = self.prev
            if self is None:
                return False
        return self.tick >= other.tick


    def __lte__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __gte__(self, other):
        return self.__eq__(other) or self.__gt__(other)

    def __contains__(self, node):
        return self.find(node) is not None

    def find(self, node):
        """Return the position of a node in the node chain."""
        res = 0
        while self is not None:
            if self.node == node:
                return res
            res += 1
            self = self.prev
        return None

    def filter(self, node, dropped=None):
        """Return an event chain without the given node.

        If the node is not in the chain, the result is not a copy.
        """
        if self.node == node:
            if dropped is not None:
                dropped(self)
            return self.prev
            # Invariant: a node can only be in the chain once
            # Thus we can stop filtering after we encounter it.
        if self.prev is None:
            return self
        prev = self.prev.filter(node, dropped=dropped)
        if prev is self.prev:
            # No change, so return unmodified
            return self
        return NodeEvent(node=self.node, tick=self.tick, prev=prev)
        
    def serialize(self, nchain=100) -> dict:
        if not nchain:
            raise RuntimeError("A chopped NodeEvent must not be set at all")
        res = attrdict(node = self.node.name)
        if self.tick is not None:
            res.tick = self.tick
        if self.prev is None:
            res.prev = None
        elif nchain > 1:
            res.prev = self.prev.serialize(nchain-1)
        return res

    @classmethod
    def deserialize(cls, msg, cache, check_dup=True):
        if msg is None:
            return None
        msg = msg.get('chain',msg)
        tick = msg.get('tick', None)
        self = cls(node=Node(msg['node'], tick=tick, cache=cache), tick=tick, check_dup=check_dup)
        if 'prev' in msg:
            self.prev = cls.deserialize(msg['prev'], cache=cache, check_dup=check_dup)
        return self

    def attach(self, prev: NodeEvent=None, dropped=None):
        """Copy this node, if necessary, and attach a filtered `prev` chain to it"""
        if prev is not None:
            prev = prev.filter(self.node, dropped=dropped)
        if self.prev is not None or prev is not None:
            self = NodeEvent(node=self.node, tick=self.tick, prev=prev)
        return self

class _NotGiven:
    pass

class UpdateEvent:
    """Represents an event which updates something.
    """
    def __init__(self, event: NodeEvent, entry: Entry, new_value, old_value=_NotGiven):
        self.event = event
        self.entry = entry
        self.new_value = new_value
        if old_value is not _NotGiven:
            self.old_value = old_value

    def __repr__(self):
        if self.entry.chain == self.event:
            res = ""
        else:
            res = repr(self.event)+": "
        return "<%s:%s%s: %s→%s>" % (
                self.__class__.__name__,
                res,
                repr(self.entry),
                '-' if not hasattr(self,'old_value') else '' if self.new_value == self.entry.data else repr(self.old_value),
                '' if self.new_value == self.entry.data else repr(self.old_value),
            )

    def serialize(self, chop_path=0, nchain=2, with_old=False):
        res = self.event.serialize(nchain=nchain)
        res.path = self.entry.path[chop_path:]
        if with_old:
            res.old_value = self.old_value
            res.new_value = self.new_value
        else:
            res.value = self.new_value
        return res

    @classmethod
    def deserialize(cls, root, msg, cache):
        if 'value' in msg:
            value = msg.value
        else:
            value = msg.new_value
        event = NodeEvent.deserialize(msg, cache=cache)
        entry = root.follow(*msg.path, create=True)

        return UpdateEvent(event, entry, value)

class Entry:
    """This class represents one key/value pair
    """
    _parent: Entry = None
    name: str = None
    _path: List[str] = None
    _root: Root = None
    _data: bytes = None
    chain: NodeEvent = None

    monitors = None

    def __init__(self, name: Str, parent: Entry):
        self.name = name
        self._sub = {}
        self.monitors = set()

        if parent is not None:
            parent._add_subnode(self)
            self._parent = weakref.ref(parent)

    def _add_subnode(self, child: Entry):
        self._sub[child.name] = child

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if other is None:
            return False
        if isinstance(other, Entry):
            other = other.name
        return self.name == other

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

    @property
    def path(self):
        if self._path is None:
            parent = self.parent
            if parent is None:
                self._path = []
            else:
                self._path = parent.path + [self.name]
        return self._path

    def follow(self, *path, create = True):
        for name in path:
            child = self._sub.get(name, None)
            if child is None:
                if not create:
                    raise KeyError(name)
                child = Entry(name, self)
            self = child
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
        root = parent.root()
        self._root = weakref.ref(root)
        return root

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

    async def set_data(self, event: NodeEvent, data: Any, local: bool = False, dropped=None):
        """This entry is updated by that event.

        Args:
          ``event``: The :cls:`NodeEvent` to base the update on.
          ``local``: Flag whether the event should be forwarded to watchers.

        Returns:
          The :cls:`UpdateEvent` that has been generated and applied.
        """
        event = event.attach(self.chain, dropped=dropped)
        evt = UpdateEvent(event, self, data, self._data)
        await self.apply(evt, local=local)
        return evt

    async def apply(self, evt:UpdateEvent, local: bool = False, dropped=None):
        """Apply this :cls`UpdateEvent` to me.
        
        Also, forward to watchers (unless ``local`` is set).
        """
        if evt.event == self.chain:
            assert self._data == evt.new_value, ("has:",self._data, "but should have:",evt.new_value)
            return
        if self.chain > evt.event: # already superseded
            return
        if not (self.chain < evt.event):
            logger.warn("*** inconsistency TODO ***")
            logger.warn("Node: %s", self)
            logger.warn("Current: %s", self.chain)
            logger.warn("New: %s", evt.event)
            return

        if not hasattr(evt, 'old_value'):
            evt.old_value = self._data
        if hasattr(evt, 'new_value'):
            self._data = evt.new_value
        else:
            self._data = evt.value
        if dropped is not None and self.chain is not None:
            dropped(evt.event, self.chain)
        self.chain = evt.event

        c = self.chain
        while c is not None:
            c.node.seen(c.tick, self)
            c = c.prev
        await self.updated(evt)

    async def walk(self, proc):
        """Call ``proc`` on this node and all its children)."""
        await proc(self)
        for v in list(self._sub.values()):
            await v.walk(proc)

    def serialize(self, chop_path=0, nchain=2):
        """Serialize this entry for msgpack.

        Args:
          ``chop_path``: If <0, do not return the entry's path.
                         Otherwise, do, but remove the first N entries.
          ``nchain``: how many change events to include.
        """
        res = attrdict(value=self._data)
        if self.chain is not None and nchain > 0:
            res.chain = self.chain.serialize(nchain=nchain)
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
                if q._distkv__free > 1:
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
                    await q.aclose()
            node = node.parent
            if node is None:
                break


    def log(self):
        """
        Returns an async context manager plus async iterator which reports
        all updates to this node.

        All concurrent loggers receive all updates.
        """
        return _RootLog(self)

    _counter = 0

    @property
    def counter(self):
        self._counter += 1
        return self._counter

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

    def __init__(self, root: Entry):
        self.root = root

    async def __aenter__(self):
        if self.q is not None:
            raise RuntimeError("You cannot enter this context more than once")
        self.q = anyio.create_queue(self.q_len)
        self.q._distkv__free = self.q_len
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
        res = await self.q.get()
        if res is None:
            raise RuntimeError("Aborted. Queue filled?")
        self.q._distkv__free += 1
        return res

