"""
This module implements a way to store error messages in MoaT-KV,
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

* first_seen

  The timestamp (unixtime) when the problem first occurred. Must not be
  modified; used for strict unique-ification.

* last_seen

  The timestamp (unixtime) when the problem last occurred.

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

* comment

  The ``repr`` of the error, or an explicit commenting text.

* message

  Some textual explanation of the error.

* data

  Any additional data required to reproduce the problem; e.g. if a
  stored procedure triggered an exception, the location of the actual code
  and the parameters used when invoking it.

The message text must be fixed. It must be format-able using the "data"
of this record.

"""

import logging
import traceback
from collections import defaultdict
from time import time  # wall clock, intentionally
from weakref import WeakValueDictionary

import anyio
from moat.util import Cache, NotGiven, Path

from .codec import packer
from .exceptions import ServerError
from .obj import AttrClientEntry, ClientEntry, ClientRoot

logger = logging.getLogger(__name__)


try:
    ClosedResourceError = anyio.exceptions.ClosedResourceError
except AttributeError:
    ClosedResourceError = anyio.ClosedResourceError


class ErrorSubEntry(AttrClientEntry):
    """
    Tracks the latest occurrence of an error, per node.
    """

    ATTRS = "seen tock trace str data comment message".split()

    def child_type(self, name):  # pylint: disable=arguments-differ
        logger.warning("Unknown entry type at %r: %s", self._path, name)
        return ClientEntry

    def __repr__(self):
        return "‹%s %s %d›" % (
            self.__class__.__name__,
            self._path[-3:],
            getattr(self, "tock", -1),
        )

    async def set_value(self, value):
        await super().set_value(value)
        if value is NotGiven:
            p = self.parent
            if p is not None:
                await self.parent.check_move()


class ErrorEntry(AttrClientEntry):
    """
    A specific error. While it's recorded per node+tock, for uniqueness,
    the error is really unique per subsystem and path.
    """

    ATTRS = "path subsystem severity resolved created count first_seen last_seen message".split()
    resolved = None  # bool; if None, no details yet
    created = None
    count = 0
    subsystem = None
    first_seen = None
    last_seen = None
    path = ()
    _real_entry = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.node, self.tock = self.subpath[-2:]

    def __repr__(self):
        return "<%s: %s %s: %s>" % (
            self.__class__.__name__,
            "/".join(str(x) for x in self.subpath),
            self.subsystem,
            " ".join(str(x) for x in self.path),
        )

    @classmethod
    def child_type(cls, name):
        return ErrorSubEntry

    @property
    def real_entry(self):
        while self._real_entry is not None:
            self = self._real_entry  # pylint: disable=self-cls-assignment
        return self

    async def check_move(self):
        dest = self.real_entry
        if dest is not self:
            await self.root.add_clean(self)

    async def resolve(self):
        """
        Record that this error has been resolved.
        """
        self.resolved = True
        await self.save()

    async def add_exc(self, node, exc, data, comment=None, message=None):
        """
        Store a detail record for this error.

        One per node, so we don't try to avoid collisions.

        Arguments:
          node (str): The node the error occurred in.
          exc (Exception): The actual exception
          data (dict): any relevant data to reproduce the problem.
        """
        res: ErrorSubEntry = self.get(node)
        if res is None:
            res = self.allocate(node)
        if True:  # pylint: disable=using-constant-test  # just for the pylint block
            # pylint: disable=attribute-defined-outside-init
            res.seen = time()
            res.tock = await self.root.client.get_tock()
            res.comment = comment or repr(exc)

        if exc is not None:
            t = traceback.format_exception(type(exc), exc, exc.__traceback__)
            if len(t) > 40:
                t = t[:10] + ["…\n"] + t[:30]
            res.trace = "".join(t)  # pylint: disable=attribute-defined-outside-init
        if message is not None:
            res.message = message  # pylint: disable=attribute-defined-outside-init
        if data is not None:
            res.data = data  # pylint: disable=attribute-defined-outside-init

        if message:
            if data:
                try:
                    m = message.format(exc=exc, **data)
                except Exception as exc:  # pylint: disable=unused-argument  # OH COME ON
                    m = message + f" (FORMAT {exc !r})"
            else:
                m = message
            if m:
                if exc:
                    m += f": {exc!r}"
            elif exc:
                m = repr(exc)
            elif comment:
                m = comment
            elif data:
                m = repr(data)
            logger.warning("Error %r %s: %s", self._path, node, m)

        try:
            r = await res.save()
        except TypeError:
            for k in res.ATTRS:
                v = getattr(res, k, None)
                try:
                    packer(v)
                except TypeError:
                    setattr(res, k, repr(v))
            r = await res.save()
        return r

    async def add_comment(self, node, comment, data):
        """
        Store this comment, typically used when something resumes working.
        One per node, so we don't need to avoid collisions.
        """
        res = dict(
            seen=time(),
            tock=await self.root.client.get_tock(),
            comment=comment,
            data=data,
        )
        logger.info("Comment %s: %s", node, comment)
        await self.root.client.set(self._path, chain=self.chain, value=res)

    async def delete(self):  # pylint: disable=signature-differs,arguments-differ
        """
        Delete myself from storage.

        This doesn't do anything locally, the watcher will get it.
        """
        await self.root._pop(self)
        try:
            return await super().delete(chain=False)
        except ServerError as exc:
            if "is new" not in repr(exc):
                raise

    async def move_to_real(self):
        """
        Move this entry, or rather the errors in it, to another.

        This is used for collision resolution.
        """

        dest = self.real_entry
        # logger.warning("DEL 1 %r %r",self,dest)
        assert dest is not self, self
        kid = self.get(self.root.name)
        if kid is not None:
            dkid = dest.get(self.root.name)
            # logger.warning("DEL 2 %r %r %r %r",self,dest,kid,dkid)
            val = kid.get_value()
            if dkid is None:
                dkid = dest.allocate(self.root.name)
                await dkid.set_value(val)
                await dkid.save()
            elif getattr(dkid, "tock", 0) < getattr(kid, "tock", 0):
                await dkid.set_value(val)
                await dkid.save()
            # logger.warning("DEL 3 %r %r %r",self,dest,dkid)
            await kid.delete()
        if not len(self):
            # logger.warning("DEL 4 %r",self)
            await self.delete()
        # logger.warning("DEL 5 %r",self)

    async def set_value(self, value):
        """Overridden: set_value

        Stores a pointer to the error in the root and keeps the records unique
        """

        await self.root._pop(self)
        if value is NotGiven:
            if self.value is NotGiven:
                return
            keep = await self.root.get_error_record(
                self.subsystem, self.path, create=False
            )
            if keep is not None:
                self._real_entry = keep.real_entry
                await self.move_to_real()
            await super().set_value(value)
            return

        await super().set_value(value)

        drop, keep = await self.root._unique(self)
        # logger.debug("UNIQ %r %r %r %s/%s",self,drop,keep,
        # "x" if drop is None else drop.subpath[0],self.root.name)
        if drop is not None:
            # self.root._dropped(drop)
            drop._real_entry = keep

            if drop.subpath[0] == self.root.name:
                await drop.move_to_real()
            # TODO remember to do it anyway, after next tick and when we're it
        self.root._push(self.real_entry)


class ErrorStep(ClientEntry):
    """
    Errors are stored at /tock/node; this represents the /tock part
    """

    @classmethod
    def child_type(cls, name):
        return ErrorEntry


def _defaultdict_init(*a, **k):
    return defaultdict(_defaultdict_init, *a, **k)


class ErrorRoot(ClientRoot):
    """
    This class represents the root of an error handling hierarchy. Ideally
    there should only be one, but you can use more if necessary.

    You typically don't create this class directly; instead, call
    :meth:`ClientRoot.as_handler`::

        errs = await ErrorRoot.as_handler(client, your_config.get("error-handler",{})

    Configuration:

    Arguments:
      prefix (list): Where to store the error data in MoaT-KV.
        The default is ``('.moat','kv','error')``.
    """

    CFG = "errors"

    def __init__(self, *a, name=None, **kw):
        super().__init__(*a, **kw)
        self.name = name or self.client.client_name
        self._loaded = anyio.Event()
        self._errors = defaultdict(dict)  # node > tock > Entry
        self._active = defaultdict(dict)  # subsystem > path > Entry
        self._done = defaultdict(WeakValueDictionary)  # subsystem > path > Entry
        self._latest = Cache(100)
        self._to_clean = set()

    @classmethod
    def child_type(cls, name):
        return ErrorStep

    async def add_clean(self, entry):
        # self._to_clean.add(entry)
        pass
        # TODO run cleanup code that consolidates these errors when we get a TagMessage
        # TODO use a weakset

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

    async def get_error_record(self, subsystem, path, *, create=True):
        """Retrieve or generate an error record for a particular subsystem
        and path.

        If ``create`` is set, the record may be incomplete and
        must be filled and stored by the caller.
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
        return self.follow(Path(self.name, tock), create=True)

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
        other = await self.get_error_record(entry.subsystem, entry.path, create=False)
        if other is None or other is entry:
            return None, None

        def _n(x):
            return 99999999999999 if x is None else x

        if _n(entry.first_seen) < _n(other.first_seen):
            return other, entry
        elif _n(other.first_seen) < _n(entry.first_seen):
            return entry, other

        if entry.node < other.node:
            return other, entry
        elif other.node < entry.node:
            return entry, other

        raise RuntimeError(f"This cannot happen: {entry.node} {entry.tock}")

    async def record_working(  # pylint: disable=dangerous-default-value
        self, subsystem, path, *, comment=None, data={}, force=False
    ):
        """This exception has been fixed.

        Arguments:
          subsystem (str): The subsystem with the error.
          *path: the path to the no-longer-offending entry.
          comment (str): text to enter
          data (dict): any relevant data
          force (bool): create an entry even if no error is open.
        """
        rec = await self.get_error_record(subsystem, path, create=force)
        if rec is None:
            return
        if not rec.resolved:
            rec.resolved = time()
            await rec.save()
        if comment or data:
            await rec.real_entry.add_comment(self.name, comment, data)
        return rec

    async def record_error(  # pylint: disable=dangerous-default-value
        self,
        subsystem,
        path,
        *,
        exc=None,
        data={},
        severity=0,
        message=None,
        force: bool = False,
        comment: str = None,
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
          message (str): some text to add to the error. It is formatted
            with the data when printed.
        """
        rec = await self.get_error_record(subsystem, path)
        if not force and hasattr(rec, "severity") and rec.severity < severity:
            return rec

        rec.severity = severity
        rec.subsystem = subsystem
        rec.path = path
        rec.resolved = False
        rec.count += 1
        rec.last_seen = time()
        if not hasattr(rec, "first_seen"):
            rec.first_seen = rec.last_seen

        try:
            await rec.save()
        except ClosedResourceError:
            logger.exception(
                "Could not save error %s %s: %s %r",
                subsystem,
                path,
                message,
                exc,
                exc_info=exc,
            )
            return  # owch, but can't be helped

        r = await rec.real_entry.add_exc(
            self.name, exc=exc, data=data, comment=comment, message=message
        )
        return r

    async def _pop(self, entry):
        """Override to deal with entry changes"""
        if entry.subsystem is None or entry.path is None:
            return
        rec = await self.get_error_record(entry.subsystem, entry.path, create=False)
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

        if entry.resolved:
            dest = self._done
            self._latest.keep(entry)
        else:
            dest = self._active
        dest[entry.subsystem][entry.path] = entry
