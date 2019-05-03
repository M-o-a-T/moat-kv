"""
This module implements a way to store error messages in DistKV,
and of course to remove or disable them when the error is gone.

Errors are implemented by storing relevant information at ``(*PREFIX,node,tock)``.
The prefix defaults to ``("errors",)``; subsystems may want to create their
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

from .util import PathLongener

CFG = dict (
        prefix=("error",),
    )

SAVED = "path subsystem severity resolved created count last_seen message".split()
SAVED_DETAIL = "seen tock trace str data".split()

async def get_error_handler(client, cfg={}):
    """Return the error handler for this client.
    
    The handler is created if it doesn't exist.
    """
    c = {}
    c.update(CFG)
    c.update(cfg)
    async def make():
        return Errors(client, c)

    return await client.unique_helper(c['prefix'], make)


class ErrorEntry:
    deleted = False
    resolved = None  # bool; if None, no details yet

    def __init__(self, store, node, tock):
        self._store = store
        self._node = node
        self._tock = tock
        self._details = {}
        self._seen = anyio.create_event()
        self.count = 0
        self.created = time()

    async def wait_seen(self):
        """
        """
        if self._seen is None:
            return
        await self._seen.wait()
        self._seen = None

    async def save(self):
        """Save myself to storage.
        
        We ignore collisions, as they don't matter: the only "problem" is
        that the count might be too low.
        """
        res = {}
        for attr in SAVED:
            try:
                v = getattr(self, attr)
            except AttributeError:
                pass
            else:
                res[attr] = v
        await self._store._client.set(*(self._store._path + (self.subsystem, self._tock)), value=res)

    async def resolve(self):
        self.resolved = True
        await self.save()

    async def add_exc(self, node, exc, data):
        """
        Store this exception detail record. It'll come back to us, thus we
        don't save it locally. One per node, so we don't try to avoid
        collisions.
        """
        res = dict(
                seen=time(),
                tock=await self._store._client.get_tock(),
                trace=traceback.format_exception(type(exc), exc, exc.__traceback__),
                str=repr(exc), 
                data=data,
            )
        await self._store._client.set(*self._store._path, self._tock, node, value=res)


    async def delete(self):
        """
        Delete myself from storage.

        This doesn't do anything locally, the watcher will get it.
        """
        await self._store._client.set(*self._store._path, self._tock, value=None)
        

    async def _set(self, val):
        """Error data arrives"""
        if val is None:
            self.deleted = True
            self._store._drop(self)
            return
        for k,v in val.items():
            setattr(self,k,v)
        self._store._update(self)

        if self._seen is not None:
            await self._seen.set()
            self._seen = None

    def _add(self, node, val):
        """An error entry from this node arrives"""
        if val is None:
            self._details.pop(node, None)
            return
        self._details[node] = val


class Errors:
    def __init__(self, client, cfg={}):
        self._client = client
        self._name = client.name
        self._path = cfg['prefix']
        self._cfg = cfg
        self._loaded = anyio.create_event()
        self._errors = defaultdict(dict)  # node > tock > Entry
        self._active = defaultdict(dict)  # subsystem > path > Entry
        self._done = defaultdict(WeakValueDictionary)  # subsystem > path > Entry
        self._latest = deque()
        self._latest_len = 10

    def all_errors(self, subsystem=None):
        """
        Iterate over all active errors, either a single subsystem or all of
        them.

        Arguments:
          subsystem (str): The subsystem to filter for.
        """
        def _all_errors():
            for s in list(self._active.values()):
                yield from s.values()

        if subsystem is not None:
            return list(self._active[subsystem].values())
        return _all_errors()

    async def get_error_record(self, subsystem, *path):
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
        tock = await self._client.get_tock()
        return ErrorEntry(self, self._name, tock)

    async def record_exc(self, subsystem, *path, exc=None, reason=None,
            data={}, severity=0, message=None):
        if message is None:
            message = repr(exc)

        rec = await self.get_error_record(subsystem, *path)
        rec.severity = severity
        rec.subsystem = subsystem
        rec.path = path
        rec.resolved = False
        rec.message = message
        rec.count += 1
        rec.last_seen = time()

        await rec.save()
        await rec.add_exc(self._client.node, exc, data)
        return rec

    def _update(self, entry):
        if not hasattr(entry,'subsystem'):
            return

        s = self._active[entry.subsystem].pop(entry.path, None)
        if s is None:
            s = self._done[entry.subsystem].pop(entry.path, None)

        if entry.resolved:
            self._done[entry.subsystem][entry.path] = entry
        else:
            self._active[entry.subsystem][entry.path] = entry

    def _drop(self, entry):
        """
        Mark this entry as deleted.

        It will still be accessible via the "._done" list for some time.
        """
        if not hasattr(entry,'subsystem'):
            return

        entry.resolved = True
        self._latest.append(entry)
        if len(self._latest) > self._latest_len:
            self._latest.popleft()
        self._active[entry.subsystem].pop(entry.path, None)
        self._done[entry.subsystem][entry.path] = entry

    async def __aenter__(self):
        await self._client.tg.spawn(self._run)
        # We do not wait for that to signal, because we set _loaded when
        # the data is there, and wait for that in individual ops
        # await self._loaded.wait()
        return self

    async def __aexit__(self, *tb):
        # Nothing to do, because the client will shut down the taskgroup
        # when it exits
        pass

    async def wait_loaded(self):
        await self._loaded.wait()

    async def _run(self):
        pl = PathLongener(())
        async with self._client.watch(*self._path, fetch=True, nchain=0, min_depth=2, max_depth=3) as watcher:
            async for msg in watcher:
                if msg.get('state', "") == "uptodate":
                    await self._loaded.set()
                    continue
                pl(msg)
                await self._run_one(msg)

    async def _run_one(self, msg):
        err = self._errors[msg.path[0]].get(msg.path[1], None)
        if err is None:
            err = ErrorEntry(self, *msg.path[0:2])
        if len(msg.path) == 2:
            await err._set(msg.value)
        else:
            err._add(msg.path[2], msg.value)

