"""
This module's job is to run code, resp. to keep it running.


"""

import anyio
from weakref import ref
from asyncactor import NodeList
from asyncactor import PingEvent, TagEvent, UntagEvent, AuthPingEvent
from copy import deepcopy
import psutil
import time
from collections.abc import Mapping

try:
    from contextlib import AsyncExitStack
except ImportError:
    from async_exit_stack import AsyncExitStack

from .actor import ClientActor
from .actor import DetachedState, PartialState, CompleteState, ActorState, BrokenState
from .util import NotGiven, combine_dict, attrdict, P, Path, logger_for, spawn, digits

from .exceptions import ServerError
from .obj import AttrClientEntry, ClientRoot

import logging

logger = logging.getLogger(__name__)

QLEN = 10


class NotSelected(RuntimeError):
    """
    This node has not been selected for a very long time. Something is amiss.
    """

    pass


class ErrorRecorded(RuntimeError):
    pass


class RunnerMsg(ActorState):
    """Superclass for runner-generated messages.

    Not directly instantiated.

    This message and its descendants take one opaque parameter: ``msg``.
    """

    pass


class ChangeMsg(RunnerMsg):
    """A message telling your code that some entry has been updated.

    Subclass this and use it as `CallAdmin.watch`'s ``cls`` parameter for easier
    disambiguation.

    The runner sets ``path`` and ``value`` attributes.
    """

    pass


class MQTTmsg(RunnerMsg):
    """A message transporting some MQTT data.

    `value` is the MsgPack-decoded content. If that doesn't exist the
    message is not decodeable.

    The runner also sets the ``path`` attribute.
    """

    pass


class ReadyMsg(RunnerMsg):
    """
    This message is queued when the last watcher has read all data.
    """

    pass


class TimerMsg(RunnerMsg):
    """
    A message telling your code that a timer triggers.

    Subclass this and use it as `CallAdmin.timer`'s ``cls`` parameter for easier
    disambiguation.
    """

    pass


_CLASSES = attrdict()
for _c in (
    DetachedState,
    PartialState,
    CompleteState,
    ActorState,
    BrokenState,
    NotGiven,
    TimerMsg,
    ReadyMsg,
    ChangeMsg,
    MQTTmsg,
    RunnerMsg,
    ErrorRecorded,
):
    _CLASSES[_c.__name__] = _c


class CallAdmin:
    """
    This class collects some standard tasks which async DistKV-embedded
    code might want to do.
    """

    _taskgroup = None
    _stack = None

    def __init__(self, runner, state, data):
        self._runner = runner
        self._state = state
        self._data = data
        self._client = runner.root.client
        self._err = runner.root.err
        self._q = runner._q
        self._path = runner._path
        self._subpath = runner.subpath
        self._n_watch = 0
        self._n_watch_seen = 0
        self._logger = runner._logger

    async def _run(self, code, data):
        """Called by the runner to actually execute the code."""
        self._logger.debug("Start %r with %r", self._runner._path, self._runner.code)
        async with anyio.create_task_group() as tg:
            self._taskgroup = tg
            async with AsyncExitStack() as stack:
                self._stack = stack
                sc = tg.cancel_scope

                self._taskgroup = tg
                self._runner.scope = sc
                data["_self"] = self

                oka = getattr(self._runner, "ok_after", 0)
                if oka > 0:

                    async def is_ok(oka):
                        await anyio.sleep(oka)
                        await self.setup_done()

                    await tg.spawn(is_ok, oka)

                await self._runner.send_event(ReadyMsg(0))
                res = code(**data)
                if code.is_async is not None:
                    res = await res

                await sc.cancel()
                return res

    async def cancel(self):
        """
        Cancel the running task
        """
        await self._taskgroup.cancel_scope.cancel()

    async def spawn(self, proc, *a, **kw):
        """
        Start a background subtask.

        The task is auto-cancelled when your code ends.

        Returns: an `anyio.abc.CancelScope` which you can use to cancel the
            subtask.
        """
        return await spawn(self._taskgroup, proc, *a, **kw)

    async def setup_done(self, **kw):
        """
        Call this when your code has successfully started up.
        """
        self._state.backoff = 0
        await self._state.save()
        await self._err.record_working("run", self._runner._path, **kw)

    async def error(self, path=None, **kw):
        """
        Record that an error has occurred. This function records specific
        error data, then raises `ErrorRecorded` which the code is not
        supposed to catch.

        See `distkv.errors.ErrorRoot.record_error` for keyword details. The
        ``path`` argument is auto-filled to point to the current task.
        """
        if path is None:
            path = self._path
        r = await self._err.record_error("run", path, **kw)
        await self._err.root.wait_chain(r.chain)
        raise ErrorRecorded()

    async def open_context(self, ctx):
        return await self._stack.enter_async_context(ctx)

    async def watch(self, path, cls=ChangeMsg, **kw):
        """
        Create a watcher. This path is monitored as per `distkv.client.Client.watch`;
        messages are encapsulated in `ChangeMsg` objects.
        A `ReadyMsg` will be sent when all watchers have transmitted their
        initial state.

        By default a watcher will only monitor a single entry. Set
        ``max_depth`` if you also want child entries.

        By default a watcher will not report existing entries. Set
        ``fetch=False`` if you want them.
        """

        class Watcher:
            """Helper class for watching an entry"""

            def __init__(self, admin, runner, client, cls, path, kw):
                kw.setdefault("fetch", True)

                self.admin = admin
                self.runner = runner
                self.client = client
                self.path = path
                self.kw = kw
                self.cls = cls
                self.scope = None

            async def run(self):
                async with anyio.open_cancel_scope() as sc:
                    self.scope = sc
                    async with self.client.watch(path, **kw) as watcher:
                        async for msg in watcher:
                            if "path" in msg:
                                chg = cls(msg)
                                try:
                                    chg.value = (  # pylint:disable=attribute-defined-outside-init
                                        msg.value
                                    )
                                except AttributeError:
                                    pass
                                chg.path = (  # pylint:disable=attribute-defined-outside-init
                                    msg.path
                                )
                                await self.runner.send_event(chg)

                            elif msg.get("state", "") == "uptodate":
                                self.admin._n_watch_seen += 1
                                if self.admin._n_watch_seen == self.admin._n_watch:
                                    await self.runner.send_event(
                                        ReadyMsg(self.admin._n_watch_seen)
                                    )

            async def cancel(self):
                if self.scope is None:
                    return False
                sc, self.scope = self.scope, None
                await sc.cancel()

        if isinstance(path, (tuple, list)):
            path = Path.build(path)
        elif not isinstance(path, Path):
            raise RuntimeError("You didn't pass in a path")

        kw.setdefault("max_depth", 0)
        if kw.setdefault("fetch", True):
            self._n_watch += 1

        w = Watcher(self, self._runner, self._client, cls, path, kw)
        await self._taskgroup.spawn(w.run)
        return w

    async def send(self, path, value=NotGiven, raw=None):
        """
        Publish an MQTT message.

        Set either ``value`` or ``raw``.
        """
        if isinstance(path, (tuple, list)):
            path = Path.build(path)
        elif not isinstance(path, Path):
            raise RuntimeError("You didn't pass in a path")

        await self._client.msg_send(topic=path, data=value, raw=raw)

    async def set(self, path, value, chain=NotGiven):
        """
        Set a DistKV value.
        """
        if isinstance(path, (tuple, list)):
            path = Path.build(path)
        elif not isinstance(path, Path):
            raise RuntimeError("You didn't pass in a path")

        return await self._client.set(path, value=value, chain=chain)

    async def get(self, path, value):
        """
        Get a DistKV value.
        """
        if isinstance(path, (tuple, list)):
            path = Path.build(path)
        elif not isinstance(path, Path):
            raise RuntimeError("You didn't pass in a path")

        res = await self._client.get(path, value=value)  # TODO chain

        if "value" in res:
            return res.value
        else:
            return KeyError(path)

    async def monitor(self, path, cls=MQTTmsg, **kw):
        """
        Create an MQTT monitor.
        Messages are encapsulated in `MQTTmsg` objects.

        By default a monitor will only monitor a single entry. You may use
        MQTT wildcards.

        The message is decoded and stored in the ``value`` attribute unless
        it's either undecodeable or ``raw`` is set, in which case it's
        stored in ``.msg``. The topic the message was sent to is in
        ``topic``.
        """

        class Monitor:
            """Helper class for reading an MQTT topic"""

            def __init__(self, admin, runner, client, cls, path, kw):
                self.admin = admin
                self.runner = runner
                self.client = client
                self.path = path
                self.kw = kw
                self.cls = cls
                self.scope = None

            async def run(self):
                async with anyio.open_cancel_scope() as sc:
                    self.scope = sc
                    async with self.client.msg_monitor(path, **kw) as watcher:
                        async for msg in watcher:
                            if "topic" in msg:
                                # pylint:disable=attribute-defined-outside-init
                                chg = cls(msg.get("raw", None))
                                try:
                                    chg.value = msg.data
                                except AttributeError:
                                    pass
                                chg.path = Path.build(msg.topic)
                                await self.runner.send_event(chg)

            async def cancel(self):
                if self.scope is None:
                    return False
                sc, self.scope = self.scope, None
                await sc.cancel()

        if isinstance(path, (tuple, list)):
            path = Path.build(path)
        elif not isinstance(path, Path):
            raise RuntimeError("You didn't pass in a path")

        w = Monitor(self, self._runner, self._client, cls, path, kw)
        await self._taskgroup.spawn(w.run)
        return w

    async def timer(self, delay, cls=TimerMsg):
        class Timer:
            def __init__(self, runner, cls, tg):
                self.runner = runner
                self.cls = cls
                self.scope = None
                self._taskgroup = tg
                self.delay = None

            async def _run(self):
                async with anyio.open_cancel_scope() as sc:
                    self.scope = sc
                    await anyio.sleep(self.delay)
                    self.scope = None
                    await self.runner.send_event(self.cls(self))

            async def cancel(self):
                if self.scope is None:
                    return False
                sc, self.scope = self.scope, None
                await sc.cancel()
                return True

            async def run(self, delay):
                await self.cancel()
                self.delay = delay
                if self.delay > 0:
                    await self._taskgroup.spawn(t._run)

        t = Timer(self._runner, cls, self._taskgroup)
        await t.run(delay)
        return t


class RunnerEntry(AttrClientEntry):
    """
    An entry representing some hopefully-running code.

    The code will run some time after ``target`` has passed.
    On success, it will run again ``repeat`` seconds later (if >0).
    On error, it will run ``delay`` seconds later (if >0), multiplied by 2**backoff.

    Arguments:
        code (list): pointer to the code that's to be started.
        data (dict): additional data for the code.
        delay (float): time before restarting the job on error.
            Default 100.
        repeat (float): time before restarting on success.
            Default: zero: no restart.
        target (float): time the job should be started at.
            Default: zero: don't start.
        ok_after (float): the job is marked OK if it has run this long.
            Default: zero: the code will do that itself.
        backoff (float): Exponential back-off factor on errors.
            Default: 1.1.

    The code runs with these additional keywords::

        _self: the `CallEnv` object, which the task can use to actually do things.
        _client: the DistKV client connection.
        _info: a queue which the task can use to receive events. A message of
            ``None`` signals that the queue was overflowing and no further
            messages will be delivered. Your task should use that as its
            mainloop.
        _P: build a path from a string
        _Path: build a path from its arguments

    Some possible messages are defined in :mod:`distkv.actor`.
    """

    ATTRS = "code data delay ok_after repeat backoff target".split()

    delay = 100  # timedelta, before restarting
    repeat = 0
    target = 0
    backoff = 1.1
    ok_after = 0

    code = ()  # what to execute
    data = None
    scope = None  # scope to kill off
    _comment = None  # used for error entries, i.e. mainly Cancel
    _q = None  # send events to the running task. Async tasks only.
    _running = False  # .run is active. Careful with applying updates.
    _task = None
    retry = None

    def __init__(self, *a, **k):
        self.data = {}  # local data

        super().__init__(*a, **k)

        self._logger = logger_for(self._path)

    def __repr__(self):
        return "<%s %r:%r>" % (self.__class__.__name__, self.subpath, self.code)

    @property
    def state(self):
        return self.root.state.follow(self.subpath, create=None)

    async def run(self):
        if self.code is None:
            return  # nothing to do here

        state = self.state
        try:
            self._running = True
            try:
                self._logger.debug("Start")
                if state.node is not None:
                    raise RuntimeError(f"already running on {state.node}")
                code = self.root.code.follow(self.code, create=False)
                data = self.data
                if data is None:
                    data = {}
                else:
                    data = deepcopy(data)

                for k, v in code.value.get("default", {}).items():
                    if k not in data:
                        data[k] = v

                if code.is_async:
                    data["_info"] = self._q = anyio.create_queue(QLEN)
                data["_client"] = self.root.client
                data["_cfg"] = self.root.client._cfg
                data["_cls"] = _CLASSES
                data["_P"] = P
                data["_Path"] = Path
                data["_log"] = self._logger
                data["_digits"] = digits

                state.started = time.time()
                state.node = state.root.name

                await state.save(wait=True)
                if state.node != state.root.name:
                    raise RuntimeError("Rudely taken away from us.", state.node, state.root.name)

                data["_self"] = calls = CallAdmin(self, state, data)
                res = await calls._run(code, data)

            except BaseException as exc:
                self._logger.info("Error: %r", exc)
                raise
            else:
                self._logger.debug("End")
            finally:
                self.scope = None
                self._q = None
                t = time.time()

        except ErrorRecorded:
            # record_error() has already been called
            self._comment = None
            state.backoff = min(state.backoff + 1, 20)

        except BaseException as exc:
            c, self._comment = self._comment, None
            async with anyio.move_on_after(5, shield=True):
                r = await self.root.err.record_error(
                    "run", self._path, exc=exc, data=self.data, comment=c
                )
                if r is not None:
                    await self.root.err.wait_chain(r.chain)
            state.backoff = min(state.backoff + 1, 20)
        else:
            state.result = res
            state.backoff = 0
            await self.root.err.record_working("run", self._path)
        finally:
            async with anyio.fail_after(2, shield=True):
                if state.node == state.root.name:
                    state.node = None
                self._running = False
                state.stopped = t

                if state.backoff > 0:
                    self.retry = t + (self.backoff ** state.backoff) * self.delay
                else:
                    self.retry = None
                async with anyio.move_on_after(2, shield=True):
                    try:
                        await state.save()
                    except anyio.exceptions.ClosedResourceError:
                        pass
                    except ServerError:
                        logger.exception("Could not save")
                    await self.root.trigger_rescan()

    async def send_event(self, evt):
        """Send an event to the running process."""
        if self._q is None:
            if self._running:
                self._logger.info("Discarding %r", evt)
        elif self._q.qsize() < QLEN - 1:
            self._logger.debug("Event: %r", evt)
            await self._q.put(evt)
        elif self._q.qsize() == QLEN - 1:
            self._logger.warning("Queue full")
            await self._q.put(None)
            self._q = None
            self._logger.info("Discarding %r", evt)

    async def seems_down(self):
        state = self.state
        state.node = None
        if not self._running:
            await state.save(wait=True)

    async def set_value(self, value):
        """Process incoming value changes"""
        c = self.code
        await super().set_value(value)

        # Check whether running code needs to be killed off
        if self.scope is not None:
            if value is NotGiven or c is not self.code:
                # The code changed.
                self._comment = "Cancel: Code changed"
                await self.scope.cancel()
            elif not getattr(self, "target", None):
                self._comment = "Cancel: target zeroed"
                await self.scope.cancel()

        await self.root.trigger_rescan()

    async def seen_value(self):
        await super().seen_value()
        await self.root.trigger_rescan()

    async def run_at(self, t: float):
        """Next run at this time.
        """
        self.target = t
        if not self._running:
            await self.save()

    def should_start(self):
        """Tell whether this job might want to be started.

        Returns:
          ``False``: No, it's running (or has run and doesn't restart).
          ``0``: No, it should not start
          ``>0``: timestamp at which it should start, or should have started

        """

        state = self.state
        if self.code is None:
            return False, "no code"
        if state.node is not None:
            return False, "node set"
        if state.started and not state.stopped:
            raise RuntimeError("Running! should not be called")

        if self.target is None:
            return False, "no target"
        elif self.target > state.started:
            return self.target, "target > started"
        elif state.backoff:
            return state.stopped + self.delay * (self.backoff ** state.backoff), "backoff"
        elif self.repeat:
            return state.stopped + self.repeat, "repeat"
        elif state.started:
            return False, "is started"
        else:
            return 0, "no target"

    def __hash__(self):
        return hash(self.subpath)

    def __eq__(self, other):
        other = getattr(other, "subpath", other)
        return self.subpath == other

    def __lt__(self, other):
        other = getattr(other, "subpath", other)
        return self.subpath < other

    @property
    def age(self):
        return time.time() - self.state.started


class RunnerNode:
    """
    Represents all nodes in this runner group.

    This is used for load balancing and such. TODO.
    """

    seen = 0
    load = 999

    def __new__(cls, root, name):
        try:
            self = root._nodes[name]
        except KeyError:
            self = object.__new__(cls)
            self.root = root
            self.name = name
            root._nodes[name] = self
        return self

    def __init__(self, *a, **k):
        pass


class StateEntry(AttrClientEntry):
    """
    This is the actual state associated with a RunnerEntry.
    It must only be managed by the node that actually runs the code.

    Arguments:
      started (float): timestamp when the job was last started
      stopped (float): timestamp when the job last terminated
      result (Any): the code's return value
      node (str): the node running this code
      backoff (float): on error, the multiplier to apply to the restart timeout
      computed (float): computed start time
      reason (str): reason why (not) starting
    """

    ATTRS = "started stopped computed reason result node backoff".split()

    started = 0  # timestamp
    stopped = 0  # timestamp
    node = None  # on which the code is currently running
    result = NotGiven
    backoff = 0
    computed = 0  # timestamp
    reason = ""

    @property
    def runner(self):
        return self.root.runner.follow(self.subpath, create=False)

    async def startup(self):
        try:
            self.runner
        except KeyError:
            # The code entry doesn't exist any more.
            await self.delete()
            return

        if self.node is None or self.node != self.root.name:
            return

        self.stopped = time.time()
        self.node = None
        self.backoff = min(20, self.backoff + 1)
        await self.root.runner.err.record_error(
            "run", self.runner._path, message="Runner restarted"
        )
        await self.save()

    async def stale(self):
        self.stopped = time.time()
        node = self.node
        self.node = None
        self.backoff = min(20, self.backoff + 2)
        await self.root.runner.err.record_error(
            "run",
            self.runner._path,
            message="Runner killed: {node} {state}",
            data={"node": node, "state": "offline" if self.stopped else "stale"},
        )
        await self.save()

    async def set_value(self, value):
        n = self.node

        await super().set_value(value)
        if not self.root.runner_ready:
            return
        try:
            run = self.runner
        except KeyError:
            return

        # side effect: add to the global node list
        run.root.get_node(n)

        # Check whether running code needs to be killed off
        if run.scope is None:
            return
        if self.node is not None and self.node == n:
            # Nothing changed.
            return
        elif self.node is None or n == self.root.runner.name:
            # Owch. Our job got taken away from us.
            run._comment = f"Cancel: Node set to {self.node !r}"
            await run.scope.cancel()
        elif n is not None:
            logger.warning(
                "Runner %s at %r: running but node is %s", self.root.name, self.subpath, n
            )

        await run.root.trigger_rescan()


class StateRoot(ClientRoot):
    """Base class for handling the state of entries.

    This is separate from the RunnerRoot hierarchy because the latter may
    be changed by anybody while this subtree may only be affected by the
    actual runner. Otherwise we get interesting race conditions.
    """

    _runner = None

    @classmethod
    def child_type(cls, name):
        return StateEntry

    @property
    def name(self):
        return self.client.client_name

    @property
    def runner(self):
        return self._runner()

    @property
    def runner_ready(self):
        r = self._runner
        if r is None:
            return False
        r = r()
        if r is None:
            return False
        return r.ready

    def set_runner(self, runner):
        self._runner = ref(runner)

    async def runner_running(self):
        for n in self.all_children:
            await n.startup()

    async def kill_stale_nodes(self, names):
        """States with node names in the "names" set are stale. Kill them."""
        for s in self.all_children:
            if s.node in names:
                await s.stale()

    _last_t = 0

    async def ping(self):

        t = time.time()
        if t - self._last_t >= abs(self._cfg["ping"]):
            self._last_t = t
            if self._cfg["ping"] > 0:
                val = self.value_or({}, Mapping)
                val["alive"] = t
                await self.update(val)
            else:
                await self.client.msg_send(
                    "run", {"group": self.runner.group, "time": t, "node": self.name}
                )


class _BaseRunnerRoot(ClientRoot):
    """common code for AnyRunnerRoot and SingleRunnerRoot"""

    _trigger: anyio.abc.Event = None
    _run_now_task: anyio.abc.CancelScope = None
    _tagged: bool = True

    err = None
    code = None
    ready = False
    _run_next = 0
    node_history = None
    _start_delay = None
    state = None
    _act = None

    CFG = "runner"
    SUB = None

    def __init__(self, *a, _subpath, err=None, code=None, nodes=0, **kw):
        super().__init__(*a, **kw)
        self.err = err
        self.code = code
        self._nodes = {}
        self.n_nodes = nodes
        self._trigger = anyio.create_event()
        self._x_subpath = _subpath

    @classmethod
    def child_type(cls, name):
        return RunnerEntry

    @classmethod
    async def as_handler(cls, client, subpath, cfg=None, **kw):  # pylint: disable=arguments-differ
        assert cls.SUB is not None
        if cfg is None:
            cfg_ = client._cfg["runner"]
        else:
            cfg_ = combine_dict(cfg, client._cfg["runner"])
        return await super().as_handler(client, subpath=subpath, _subpath=subpath, cfg=cfg_, **kw)

    async def run_starting(self):
        from .errors import ErrorRoot
        from .code import CodeRoot

        if self.err is None:
            self.err = await ErrorRoot.as_handler(self.client)
        if self.code is None:
            self.code = await CodeRoot.as_handler(self.client)

        await self._state_runner()
        self.state.set_runner(self)

        self.node_history = NodeList(0)
        self._start_delay = self._cfg["start_delay"]

        await super().run_starting()

    async def _state_runner(self):
        self.state = await StateRoot.as_handler(
            self.client, cfg=self._cfg, subpath=self._x_subpath, key="state"
        )

    @property
    def name(self):
        """my node name"""
        return self.client.client_name

    def get_node(self, name):  # pylint: disable=unused-argument
        """
        If the runner keeps track of "foreign" nodes, allocate them
        """
        return None

    async def running(self):
        await self._tg.spawn(self._run_actor)

        # the next block needs to be atomic
        self.ready = True

        await self.state.runner_running()
        await super().running()

    async def _run_actor(self):
        """
        This method is started as a long-lived task of the root as soon as
        the subtree's data are loaded.

        Its job is to control which tasks are started.
        """
        raise RuntimeError("You want to override me." "")

    async def trigger_rescan(self):
        """Tell the _run_actor task to rescan our job list prematurely"""
        if self._trigger is not None:
            await self._trigger.set()

    async def _run_now(self, evt=None):
        t_next = self._run_next
        async with anyio.open_cancel_scope() as sc:
            self._run_now_task = sc
            if evt is not None:
                await evt.set()
            t = time.time()
            while True:
                if t_next > t:
                    async with anyio.move_on_after(t_next - t):
                        await self._trigger.wait()
                        self._trigger = anyio.create_event()

                t = time.time()
                t_next = t + 999
                for j in self.all_children:
                    d, r = j.should_start()
                    if not d or d > t:
                        j.state.computed = d
                        j.state.reason = r
                        if self._tagged:
                            await j.state.save()
                        if d and t_next > d:
                            t_next = d
                        continue
                    await self._tg.spawn(j.run)
                    await anyio.sleep(self._start_delay)

    async def notify_actor_state(self, msg=None):
        """
        Notify all running jobs about a possible change in active status.
        """
        ac = len(self.node_history)
        if ac == 0 or msg is None:
            ac = BrokenState
        elif self.name in self.node_history and ac == 1:
            ac = DetachedState
        elif self._act is not None and ac >= self.n_nodes:  # TODO configureable
            ac = CompleteState
        else:
            ac = PartialState

        logger.debug("State %r %r", ac, msg)

        ac = ac(msg)
        for n in self.all_children:
            await n.send_event(ac)


class AnyRunnerRoot(_BaseRunnerRoot):
    """
    This class represents the root of a code runner. Its job is to start
    (and periodically restart, if required) the entry points stored under it.

    :class:`AnyRunnerRoot` tries to ensure that the code in question runs
    on one single cluster member. In case of a network split, the code will
    run once in each split areas until the split is healed.
    """

    SUB = "group"

    _stale_times = None
    tg = None
    seen_load = None
    _tagged: bool = False

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.group = (
            P(self.client.config.server.root) + P(self._cfg["name"]) | "any" | self._path[-1]
        )

    def get_node(self, name):
        return RunnerNode(self, name)

    @property
    def max_age(self):
        """Timeout after which we really should have gotten another go"""
        # allow one go-around without being in the list; happens when the
        # bus is slow
        return self._act.cycle_time_max * (self._act.history_size + 2.5)

    async def _run_actor(self):
        """
        Monitor the Actor state, run a :meth:`_run_now` subtask whenever we're 'it'.
        """
        async with ClientActor(
            self.client, self.name, topic=self.group, cfg=self._cfg["actor"]
        ) as act:
            self._act = act

            age_q = anyio.create_queue(10)
            await self.spawn(self._age_killer, age_q)

            psutil.cpu_percent(interval=None)
            await act.set_value(0)
            self.seen_load = None

            async for msg in act:
                logger.debug("Actor %r", msg)
                if isinstance(msg, PingEvent):
                    await act.set_value(100 - psutil.cpu_percent(interval=None))

                    node = self.get_node(msg.node)
                    node.load = msg.value
                    node.seen = time.time()
                    if self.seen_load is not None:
                        self.seen_load += msg.value
                    self.node_history += node

                elif isinstance(msg, TagEvent):
                    self._tagged = True
                    load = 100 - psutil.cpu_percent(interval=None)
                    await act.set_value(load)
                    if self.seen_load is not None:
                        pass  # TODO

                    self.get_node(self.name).seen = time.time()
                    self.node_history += self.name
                    evt = anyio.create_event()
                    await self.spawn(self._run_now, evt)
                    await age_q.put(None)
                    await evt.wait()

                    await self.state.ping()

                elif isinstance(msg, UntagEvent):
                    self._tagged = False
                    await act.set_value(100 - psutil.cpu_percent(interval=None))
                    self.seen_load = 0

                    await self._run_now_task.cancel()
                    # TODO if this is a DetagEvent, kill everything?

                await self.notify_actor_state(msg)

            pass  # end of actor task

    async def find_stale_nodes(self, cur):
        """
        Find stale nodes (i.e. last seen < cur) and clean them.
        """
        if self._stale_times is None:
            self._stale_times = []
        self._stale_times.append(cur)
        if self._stale_times[0] > cur - self.max_age:
            return
        if len(self._stale_times) < 5:
            return
        cut = self._stale_times.pop(0)

        dropped = []
        for node in self._nodes.values():
            if node.seen < cut:
                dropped.append(node)
        names = set()
        for node in dropped:
            names.add(node.name)
            del self._nodes[node.name]
        if names:
            await self.state.kill_stale_nodes(names)

    async def _age_killer(self, age_q):
        """
        Subtask which cleans up stale tasks

        TODO check where to use time.monotonic
        """
        t0 = None
        t1 = time.time()
        while True:
            async with anyio.move_on_after(self.max_age):
                await age_q.get()
                t1 = time.time()
            t2 = time.time()
            if t1 + self.max_age < t2:
                raise NotSelected(self.max_age, t2, t1)
            t0 = t1
            t1 = t2
            if t0 is not None:
                await self.find_stale_nodes(t0)


class SingleRunnerRoot(_BaseRunnerRoot):
    """
    This class represents the root of a code runner. Its job is to start
    (and periodically restart, if required) the entry points stored under it.

    While :class:`AnyRunnerRoot` tries to ensure that the code in question runs
    on any cluster member, this class runs tasks on a single node.
    The code is able to check whether any and/or all of the cluster's main
    nodes are reachable; this way, the code can default to local operation
    if connectivity is lost.

    Local data (dict):

    Arguments:
      cores (tuple): list of nodes whose reachability may determine
        whether the code uses local/emergency/??? mode.

    Config file:

    Arguments:
      path (tuple): the location this entry is stored at. Defaults to
        ``('.distkv', 'process')``.
      name (str): this runner's name. Defaults to the client's name plus
        the name stored in the root node, if any.
      actor (dict): the configuration for the underlying actor. See
        ``asyncactor`` for details.
    """

    SUB = "single"

    err = None
    code = None
    state = None
    tg = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.group = (
            P(self.client.config.server.root) + P(self._cfg["name"])
            | "single"
            | self._path[-2]
            | self._path[-1]
        )

    async def set_value(self, value):
        await super().set_value(value)
        try:
            cores = value["cores"]
        except (TypeError, KeyError):
            if self._act is not None:
                await self._act.disable(0)
        else:
            if self.name in cores:
                await self._act.enable(len(cores))
            else:
                await self._act.disable(len(cores))

    @property
    def max_age(self):
        """Timeout after which we really should have gotten another ping"""
        return self._act.cycle_time_max * 1.5

    async def _run_actor(self):
        async with anyio.create_task_group() as tg:
            age_q = anyio.create_queue(1)

            async with ClientActor(self.client, self.name, topic=self.group, cfg=self._cfg) as act:
                self._act = act
                await tg.spawn(self._age_notifier, age_q)
                await self.spawn(self._run_now)
                await act.set_value(0)

                async for msg in act:
                    if isinstance(msg, AuthPingEvent):
                        # Some node, not necessarily us, is "it".
                        # We're the SingleNode runner: we manage our jobs
                        # when triggered by this, no matter whether we're
                        # "it" or not.
                        self.node_history += msg.node
                        await age_q.put(None)
                        await self.notify_actor_state(msg)

                        await self.state.ping()

                pass  # end of actor task

    async def _age_notifier(self, age_q):
        """
        This background job triggers :meth:`notify_actor_state` when too much
        time has passed without an AuthPing.
        """
        while True:
            try:
                async with anyio.fail_after(self.max_age):
                    await age_q.get()
            except TimeoutError:
                await self.notify_actor_state()


class AllRunnerRoot(SingleRunnerRoot):
    """
    This class represents the root of a code runner. Its job is to start
    (and periodically restart, if required) the entry points stored under it.

    This class behaves like `SingleRunner`, except that it runs tasks on all nodes.
    """

    SUB = "all"

    err = None
    _act = None
    code = None

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.group = (
            P(self.client.config.server.root) + P(self._cfg["name"]) | "all" | self._path[-1]
        )

    async def _state_runner(self):
        self.state = await StateRoot.as_handler(
            self.client,
            cfg=self._cfg,
            subpath=self._x_subpath + (self.client.client_name,),
            key="state",
        )
