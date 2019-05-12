"""
This module implements a way to store error messages in DistKV,
and of course to remove or disable them when the error is gone.

Errors are implemented by storing relevant information at ``(*PREFIX,node,tock)``.
The prefix defaults to ``("error",)``; subsystems may want to create their
own list.

Each error is stored as a record with these arguments:

* path

  The path to the element that caused the error.

* subsystem

  A subsystem.

* severity

  0…7 corresponding to fatal/error/warning/info/note/debug/trace.

* resolved

  If set, contains the ``tock`` value when the problem was fixed.

* created

  The timestamp (unixtime) when the problem first occurred.

* count

  The number of occurrences.

* last_seen

  The timestamp (unixtime) when the problem last occurred.

* message

  Some textual explanation of the error.

(path,subsystem) must be unique.


Single error trace records may be stored below the error message. Their
name should be the node which noticed the problem, they have the following
structure:

* seen

  Timestamp

* tock

  Tock value when the problem occurred

* trace

  A multi-line textual error message

* str

  Generally the ``repr`` of the error.

* data

  Any additional data required to reproduce the problem; e.g. if a
  stored procedure triggered an exception, the location of the actual code
  and the parameters used when invoking it.

"""

import anyio
import traceback
from collections import defaultdict, deque
from weakref import WeakValueDictionary
from time import time  # wall clock, intentionally

from .client import AttrClientEntry, ClientEntry, ClientRoot
from .util import PathLongener, Cache


class ErrorSubEntry(AttrClientEntry):
    """
    Tracks the latest occurrence of an error, per node.
    """

    ATTRS = "seen tock trace str data".split()

    @classmethod
    def child_type(cls, name):
        logger.warning("Unknown entry type at %r: %s", self._path, name)
        return ClientEntry


class ErrorEntry(AttrClientEntry):
    """
    A specific error. While it's recorded per node+tock, for uniqueness,
    the error is really unique per subsystem and path.
    """

    ATTRS = "path subsystem severity resolved created count last_seen message".split()
    deleted = False
    resolved = None  # bool; if None, no details yet
    count = 0
    subsystem = None
    path = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.node, self.tock = self.subpath[-2:]

    @classmethod
    def child_type(cls, name):
        return ErrorSubEntry

    async def update(self, value):
        raise RuntimeError("Nope. Set attributes and call '.save()'.")

    async def resolve(self):
        """
        Record that this error has been resolved.
        """
        self.resolved = True
        await self.save()

    async def add_exc(self, node, exc, data, comment=None):
        """
        Store a detail record for this error.

        One per node, so we don't try to avoid collisions.

        Arguments:
          node (str): The node the error occurred in.
          exc (Exception): The actual exception
          data (dict): any relevant data to reproduce the problem.
        """
        res = dict(
            seen=time(),
            tock=await self.root.client.get_tock(),
            trace=traceback.format_exception(type(exc), exc, exc.__traceback__),
            str=comment or repr(exc),
            data=data,
        )
        try:
            await self.root.client.set(*self._path, node, value=res)
        except TypeError:
            for k, v in data.items():
                data[k] = repr(v)
            await self.root.client.set(*self._path, node, value=res)

    async def add_comment(self, node, comment, data):
        """
        Store this comment, typically used when something resumes working.
        One per node, so we don't try to avoid collisions.
        """
        res = dict(
            seen=time(),
            tock=await self._store._client.get_tock(),
            str=comment,
            data=data,
        )
        await self.root.client.set(*self._path, self._tock, node, value=res)

    async def delete(self):
        """
        Delete myself from storage.

        This doesn't do anything locally, the watcher will get it.
        """
        await self._store._client.set(*self._store._path, self._tock, value=None)
        for node in list(self._details.keys()):
            await self._store._client.set(*self._store._path, value=None)

    async def move_to(self, dest):
        """
        Move this entry, or rather the errors in it, to another.

        This is used for collision resolution.
        """
        for kid in self:
            try:
                dkid = dest[kid._name]
            except KeyError:
                dest.add(kid.value)
            else:
                if dkid.tock < kid.tock:
                    continue
                dkid.update(kid.value)
            await kid.update(None)
        await self.update(None)

    async def set_value(self, val):
        """Overridden: set_value
        
        Stores a pointer to the error in the root and keeps the records unique
        """

        await self.root._pop(self)
        await super().set_value(val)

        drop, keep = await self.root._unique(self)
        if drop is not None:
            await drop.move_to(keep)

        if drop is not self:
            self.root._push(self)


class ErrorStep(ClientEntry):
    """
    Errors are stored at /tock/node; this represents the /tock part
    """

    @classmethod
    def child_type(cls, name):
        return ErrorEntry


class ErrorRoot(ClientRoot):
    """
    This class represents the root of an error handling hierarchy. Ideally
    there should only be one, but you can use more if necessary.

    You typically don't create this class directly; instead, call
    :meth:`ClientRoot.as_handler`::

        errs = await ErrorRoot.as_handler(client, your_config.get("error-handler",{})

    Configuration:

    Arguments:
      prefix (list): Where to store the error data in DistKV.
        The default is ``('.distkv','error')``.
    """

    CFG = dict(prefix=(".distkv", "error"))

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._name = self.client.name
        self._loaded = anyio.create_event()
        self._errors = defaultdict(dict)  # node > tock > Entry
        self._active = defaultdict(dict)  # subsystem > path > Entry
        self._done = defaultdict(WeakValueDictionary)  # subsystem > path > Entry
        self._latest = Cache(100)

    @classmethod
    def child_type(cls, name):
        return ErrorStep

    def all_errors(self, subsystem=None):
        """
        Iterate over all active errors, either a single subsystem or all of
        them.

        Arguments:
          subsystem (str): The subsystem to filter for.
        """

        if subsystem is None:
            for s in list(self._active.values()):
                yield from iter(s.values())
        else:
            yield from iter(self._active[subsystem].values())

    async def get_error_record(self, subsystem, *path, create=True):
        """Retrieve or generate an error record for a particular subsystem
        and path.
        
        The record may be incomplete and must be filled and stored by the caller.
        """

        err = self._active[subsystem].get(path, None)
        if err is not None:
            return err
        err = self._done[subsystem].get(path, None)
        if err is not None:
            return err
        if not create:
            return None
        tock = await self.client.get_tock()
        return self.follow(self._name, tock)

    async def _unique(self, entry):
        """
        Test whether this record is unique.

        Returns:
          ``None,None`` if there is no problem
          Otherwise, a tuple:
              - the record that should be deleted
              - the record that should be kept

        This is used for collision resolution and **must** be stable, i.e.
        not depend on which node it is running on or which entry arrives
        first.
        """
        other = await self.get_error_record(entry.subsystem, *entry.path, create=False)
        if other is None or other is entry:
            return None, None

        if other.resolved and not entry.resolved:
            return other, entry
        elif entry.resolved and not other.resolved:
            return entry, other

        if entry.tock < other.tock:
            return other, entry
        elif other.tock < entry.tock:
            return entry, other

        if entry.node < other.node:
            return other, entry
        elif other.node < entry.node:
            return entry, other

        raise RuntimeError("This cannot happen: %s %s", entry.node, entry.tock)

    async def record_working(
        self, subsystem, *path, comment=None, data={}, force=False
    ):
        """This exception has been fixed.
        
        Arguments:
          subsystem (str): The subsystem with the error.
          *path: the path to the no-longer-offending entry.
          comment (str): text to enter
          data (dict): any relevant data
          force (bool): create an entry even if no error is open.
        """
        rec = await self.get_error_record(subsystem, *path, create=False)
        if rec is None:
            return
        if not rec.resolved:
            rec.resolved = time()
            await rec.save()
        if comment or data:
            rec.add_comment(self._name, comment, data)
        return rec

    async def record_exc(
        self,
        subsystem,
        *path,
        exc=None,
        reason=None,
        data={},
        severity=0,
        message=None,
        force: bool = False,
        comment: str = None
    ):
        """An exception has occurred for this subtype and path.
        
        Arguments:
          subsystem (str): The subsystem with the error.
          *path: the path to the no-longer-offending entry.
          exc (Exception): The exception in question.
          data (dict): any relevant data
          severity (int): error gravity.
          force (bool): Flag whether a low-priority exception should
            override a high-prio one.
          message (str): some human-readable text to add to the error.
        """
        if message is None:
            message = repr(exc)

        rec = await self.get_error_record(subsystem, *path)
        try:
            if not force and rec.severity < severity:
                return
        except AttributeError:
            pass

        rec.severity = severity
        rec.subsystem = subsystem
        rec.path = path
        rec.resolved = False
        rec.message = message
        rec.count += 1
        rec.last_seen = time()

        await rec.save()
        await rec.add_exc(self._name, exc, data, comment=comment)
        return rec

    async def _pop(self, entry):
        """Override to deal with entry changes"""
        if entry.subsystem is None or entry.path is None:
            return
        rec = await self.get_error_record(entry.subsystem, *entry.path, create=False)
        if rec is not entry:
            return

        try:
            del (self._done if entry.resolved else self._active)[entry.subsystem][
                entry.path
            ]
        except KeyError:
            pass

    def _push(self, entry):
        if entry.subsystem is None or entry.path is None:
            return
        if entry.value is None or entry.deleted:
            return

        if entry.resolved:
            dest = self._done
            self._latest.keep(entry)
        else:
            dest = self._active
        dest[entry.subsystem][entry.path] = entry
