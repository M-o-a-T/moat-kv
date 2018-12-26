# DistKV's data model

from __future__ import annotations

import weakref
import attr

from typing import Optional, List

from logging import getLogger
logger = getLogger(__name__)


class NodeDataSkipped(Exception):
    def __init__(self, node):
        self.node = node
    def __repr__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.node)

_nodes = {}

class Node:
    """Represents one DistKV participant.
    """
    name: str = None
    tick: int = None

    def __new__(cls, name, tick):
        self = _nodes.get(name, None)
        if self is None:
            self = object.__new__(cls)
            self.name = name
            _nodes[name] = self
        elif self.tick < tick:
            raise RuntimeError("Node %s has %d but sees %d" % (name, self.tick,tick))
        self.tick = tick
        return self

    def __init__(self, name, tick):
        return

class NodeEvent:
    """Represents any event originating at a node.

    Args:
      ``node``: The node thus affected
      ``tick``: Counter, timestamp, whatever
      ``prev``: The previous event, if any

    """
    def __init__(self, node:Node, tick = None, prev: NodeEvent = None):
        self.node = node
        if tick is None:
            tick = node.tick
        self.tick = tick
        self.prev = None
        if prev is not None:
            assert prev.filter(self.node) is prev
            self.prev = prev

    def __len__(self):
        if self.prev is None:
            return 1
        return 1+len(self.prev)

    def __repr__(self):
        return "<%s:%s %d>" % (self.__class__.__name__, self.node, len(self))

    def __eq__(self, other):
        return self.node == other.node and self.tick == other.tick

    def __lt__(self, other):
        if self == other:
            return False
        while self.node != other.node:
            other = other.prev
            if other is None:
                return False
            return self.tick < other.tick

    def __gt__(self, other):
        return other < self

    def __lte__(self, other):
        return self.__eq__(other) or self.__lt__(other)

    def __gte__(self, other):
        return self.__eq__(other) or self.__gt__(other)


    def filter(self, node):
        """Return an event chain without the given node.

        If the node is not in the chain, the result is not a copy.
        """
        if self.node == node:
            if self.prev is None:
                return None
            return self.prev.filter(node)
        if self.prev is None:
            return self
        prev = self.prev.filter(node)
        if prev is self.prev:
            return self
        return NodeEvent(node=self.node, tick=self.tick, prev=prev)
        
    def serialize(self, depth=0) -> dict:
        if not depth:
            return None
        res = {'node': self.node.name, 'tick': self.tick}
        if depth and self.prev:
            res['prev'] = self.prev.serialize(depth)
        return res

    def attach(self, prev: NodeEvent=None):
        """Copy this node, if necessary, and attach a filtered `prev` chain to it"""
        if prev is not None:
            prev = prev.filter(self.node)
        if self.prev is not None or prev is not None:
            self = NodeEvent(node=self.node, tick=self.tick, prev=prev)
        return self


@attr.s
class UpdateEvent:
    """Represents an event which updates something.
    """
    event: NodeEvent = attr.ib()
    entry: Entry = attr.ib()
    new_value: bytes = attr.ib()
    old_value: bytes = attr.ib()


class Entry:
    """This class represents one key/value pair
    """
    _parent: Entry = None
    name: str = None
    _path: List[str] = None
    _root: Root = None
    _data: bytes = None
    changes: NodeEvent = None

    updaters = None

    def __init__(self, name: Str, parent: Entry):
        self.name = name
        self._sub = {}
        self.updaters = set()

        if parent is not None:
            parent._add_subnode(self)
            self._parent = weakref.ref(parent)

    def _add_subnode(self, child: Entry):
        self._sub[child.name] = child

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
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
            if self.changes is not None:
                res += "@%s" % (repr(self.changes),)
            if self.data is not None:
                res += " =%s" % (repr(self.data),)
            res += ">"
        except Exception as exc:
            res = "<%s:%s" % (self.__class__.__name__, str(exc))
        return res

    @property
    def data(self):
        return self._data

    async def set_data(self, event: NodeEvent, data: bytes):
        """This entry is updated by that event.
        """
        evt = UpdateEvent(event, self, self._data, data)
        self._data = data
        self.changes = evt.event.attach(self.changes)
        await self.updated(self)

    def serialize(self, chop_path=0, depth=2):
        """Serialize this entry for msgpack.

        Args:
          ``chop_path``: If <0, do not return the entry's path.
                         Otherwise, do, but remove the first N entries.
          ``depth``: how many change events to include.
        """
        res = {'value': self._data}
        if self.changes is not None and depth > 0:
            res['changed'] = self.changes.serialize(depth=depth-1)
        if chop_path >= 0:
            path = self.path
            if chop_path > 0:
                path = path[chop_path:]
            res['path'] = path
        return res

    async def __anext__(self):
        return await self.read_q.receive()

    async def updated(self, event: UpdateEvent):
        node = self
        while True:
            bad = set()
            for q in node.updaters:
                try:
                    q.send_nowait(event)
                except trio.WouldBlock:
                    bad.add(q)
            for q in bad:
                node.updaters.remove(q)
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
    read_q = None
    write_q = None

    def __init__(self, root: Entry):
        self.root = root

    async def __aenter__(self):
        if self.read_q is not None:
            raise RuntimeError("You cannot enter this context more than once")
        self.write_q, self.read_q = trio.open_memory_channel()
        self.updaters.add(self.write_q)
        return self.read_q

    async def __aexit__(self, *tb):
        self.updaters.remove(self.write_q)
        self.read_q = self.write_q = None

    def __aiter__(self):
        if self.read_q is None:
            raise RuntimeError("You need to enclose this with 'async with'")
        return self

    async def __anext__(self):
        if self.read_q is None:
            raise RuntimeError("You need to enclose this with 'async with'")
        return await self.read_q.receive()

