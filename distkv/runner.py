"""
This module's job is to run code, resp. to keep it running.


"""

import anyio
from weakref import ref
from asyncserf.actor import NodeList
from asyncserf.actor import PingEvent, TagEvent, UntagEvent, AuthPingEvent
from copy import deepcopy
import psutil
import time

from .actor import ClientActor
from .actor import DetachedState, PartialState, CompleteState, ActorState
from .util import NotGiven

from .exceptions import ServerError
from .obj import AttrClientEntry, ClientRoot, ClientEntry

import logging

logger = logging.getLogger(__name__)

QLEN = 10


class NotSelected(RuntimeError):
    """
    This node has not been selected for a very long time. Something is amiss.
    """

    pass


class RunnerEntry(AttrClientEntry):
    """
    An entry representing some hopefully-running code.

    The code will run some time after ``target`` has passed.
    On success, it will run again ``repeat`` seconds later (if >0).
    On error, it will run ``delay`` seconds later (if >0), multiplied by 2**backoff.

    Arguments:
      code (list): pointer to the code that's to be started.
      data (dict): additional data for the code.
      delay (float): time before restarting the job on error
      repeat (float): time before restarting on success
      target (float): time the job should be started at

    The code runs with these additional keywords::

      _entry: this object
      _client: the DistKV client connection
      _info: a queue to send events to the task. A message of ``None``
          signals that the queue was overflowing and no further messages will
          be delivered.

    Messages are defined in :mod:`distkv.actor`.
    """

    ATTRS = "code data delay repeat backoff target".split()

    delay = 100  # timedelta, before restarting
    repeat = 0
    target = 0
    backoff = 1.4

    code = ()  # what to execute
    data = None
    scope = None  # scope to kill off
    _comment = None  # used for error entries, i.e. mainly Cancel
    _q = None  # send events to the running task. Async tasks only.
    _running = False  # .run is active. Careful with applying updates.
    _task = None

    def __init__(self, *a, **k):
        self.data = {}  # local data

        super().__init__(*a, **k)

    def __repr__(self):
        return "<%s %r:%r>" % (self.__class__.__name__, self.subpath, self.code)

    @property
    def state(self):
        return self.root.state.follow(*self.subpath, create=True)

    async def run(self):
        if self.code is None:
            return  # nothing to do here

        state = self.state
        try:
            self._running = True
            try:
                if state.node is not None:
                    raise RuntimeError("already running on %s", state.node)
                code = self.root.code.follow(*self.code, create=False)
                data = deepcopy(self.data)

                if code.is_async:
                    data["_info"] = self._q = anyio.create_queue(QLEN)
                    if self.root._active is not None:
                        await self._q.put(self.root._active)
                data["_entry"] = self
                data["_client"] = self.root.client

                state.started = time.time()
                state.node = state.root.name

                await state.save(wait=True)
                if state.node != state.root.name:
                    raise RuntimeError(
                        "Rudely taken away from us.", state.node, state.root.name
                    )

                logger.debug("Start %r with %r", self._path, self.code)
                async with anyio.open_cancel_scope() as sc:
                    self.scope = sc
                    res = code(**data)
                    if code.is_async is not None:
                        res = await res
                    await sc.cancel()
            finally:
                logger.debug("End %r", self._path)
                self.scope = None
                self._q = None
                t = time.time()

        except BaseException as exc:
            c, self._comment = self._comment, None
            async with anyio.move_on_after(2, shield=True):
                await self.root.err.record_error(
                    "run",
                    *self._path,
                    message="Exception",
                    exc=exc,
                    data=self.data,
                    comment=c
                )
            state.backoff += 1
        else:
            state.result = res
            state.backoff = 0
            await self.root.err.record_working("run", *self._path)
        finally:
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
                except ServerError:
                    logger.exception("Could not save")
                await self.root.trigger_rescan()

    async def send_event(self, evt):
        if self._q is not None:
            if self._q.qsize() < QLEN - 1:
                await self._q.put(evt)
            elif self._q.qsize() == QLEN - 1:
                await self._q.put(None)
                self._q = None

    async def seems_down(self):
        state = self.state
        state.node = None
        if not self._running:
            await state.save(wait=True)

    async def set_value(self, val):
        c = self.code
        await super().set_value(val)

        # Check whether running code needs to be killed off
        if self.scope is not None:
            if val is NotGiven or c != self.code:
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
          n>0: wait for n seconds before thinking again.
          n<0: should have been started n seconds ago, do something!

        """

        state = self.state
        if self.code is None:
            return False
        if state.node is not None:
            return False
        assert not state.started or state.stopped

        if self.target > state.started:
            return self.target
        elif state.backoff:
            return state.stopped + self.delay * (self.backoff ** state.backoff)
        elif self.repeat:
            return state.stopped + self.repeat
        elif state.started:
            return False
        else:
            return 0

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
    It mmust only be managed by the node that actually runs the code.

    Arguments:
      started (float): timestamp when the job was last started
      stopped (float): timestamp when the job last terminated
      result (Any): the code's return value
      node (str): the node running this code
      backoff (float): on error, the multiplier to apply to the restart timeout
    """

    ATTRS = "started stopped result node backoff".split()

    started = 0  # timestamp
    stopped = 0  # timestamp
    node = None  # on which the code is currently running
    result = NotGiven
    backoff = 0

    @property
    def runner(self):
        return self.root.runner.follow(*self.subpath, create=False)

    async def startup(self):
        try:
            run = self.runner
        except KeyError:
            # The code entry doesn't exist any more.
            await self.delete()
            return

        if self.node is None or self.node != self.root.name:
            return

        self.stopped = time.time()
        self.node = None
        self.backoff += 1
        await self.root.runner.err.record_error(
            "run", *self.runner._path, message="Runner restarted"
        )
        await self.save()

    async def stale(self):
        self.stopped = time.time()
        self.node = None
        self.backoff += 2
        await self.root.runner.err.record_error(
            "run",
            *self.runner._path,
            client_name=self.node,
            message="Runner {node} {state}",
            data={"node": self.node, "state": "offline" if self.stopped else "stale"}
        )
        await self.save()

    async def set_value(self, value):
        n = self.node

        await super().set_value(value)
        if not self.root.runner.ready:
            return
        try:
            run = self.runner
        except KeyError:
            return

        # Check whether running code needs to be killed off
        if run.scope is None:
            return
        if self.node is not None and self.node == n:
            # Nothing changed.
            return
        elif self.node is None or n == self.root.runner.name:
            # Owch. Our job got taken away from us.
            self._comment = "Cancel: Node set to %r" % (self.node,)
            await run.scope.cancel()
        elif n is not None:
            logger.warning(
                "Runner %s at %r: running but node is %s",
                self.root.name,
                self.subpath,
                n,
            )

        await run.root.trigger_rescan()


class StateRoot(ClientRoot):
    """Base class for handling the state of entries.

    This is separate from the RunnerRoot hierarchy because the latter may
    be changed by anybody while this subtree may only be affected by the
    actual runner. Otherwise we get interesting race conditions.
    """

    @classmethod
    def child_type(cls, name):
        return StateEntry

    @property
    def name(self):
        return self.client.client_name

    @property
    def runner(self):
        return self._runner()

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


class _BaseRunnerRoot(ClientRoot):
    """common code for AnyRunnerRoot and SingleRunnerRoot"""

    _active: ActorState = None
    _trigger: anyio.abc.Event = None
    _run_now_task: anyio.abc.CancelScope = None

    err = None
    code = None
    this_root = None
    ready = False
    _run_next = 0

    def __init__(self, *a, err=None, code=None, **kw):
        super().__init__(*a, **kw)
        self.err = err
        self.code = code
        self._nodes = {}
        self._trigger = anyio.create_event()

    async def run_starting(self):
        from .errors import ErrorRoot
        from .code import CodeRoot

        if self.err is None:
            self.err = await ErrorRoot.as_handler(self.client)
        if self.code is None:
            self.code = await CodeRoot.as_handler(self.client)
        self.state = await StateRoot.as_handler(self.client, cfg=self._cfg, key="state")
        self.state.set_runner(self)

        g = ["run"]
        if "name" in self._cfg:
            g.append(self._cfg["name"])
        if self.value and "name" in self.value:
            g.append(self.value["name"])
        self.group = ".".join(g)
        self.node_history = NodeList(0)
        self._start_delay = self._cfg["start_delay"]

        await super().run_starting()

    @property
    def name(self):
        """my node name"""
        return self.client.client_name

    def get_node(self, name):
        """
        If the runner keeps track of "foreign" nodes, allocate them
        """
        return None

    async def running(self):
        await self._tg.spawn(self._run_actor)

        # the next block needs to be atomic
        for n in self.this_root.all_children:
            print(n)
        self.ready = True

        await self.state.runner_running()
        await super().running()

    async def _run_actor(self):
        raise RuntimeError("You want to override me." "")

    async def trigger_rescan(self):
        """Tell the run_now task to rescan our job list"""
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
                t_next = t + 99999
                for j in self.this_root.all_children:
                    d = j.should_start()
                    if d is False:
                        continue
                    if d <= t:
                        await self._tg.spawn(j.run)
                        await anyio.sleep(self._start_delay)
                    elif t_next > d:
                        t_next = d


class AnyRunnerRoot(_BaseRunnerRoot):
    """
    This class represents the root of a code runner. Its job is to start
    (and periodically restart, if required) the entry points stored under it.
    """

    CFG = "anyrunner"

    _stale_times = None

    @classmethod
    def child_type(cls, name):
        return RunnerEntry

    def get_node(self, name):
        return RunnerNode(self, name)

    @property
    def max_age(self):
        """Timeout after which we really should have gotten another go"""
        return self._act.cycle_time_max * (self._act.history_size + 1.5)

    @property
    def this_root(self):
        return self

    async def _run_actor(self):
        async with anyio.create_task_group() as tg:
            self.tg = tg

            async with ClientActor(
                self.client, self.name, prefix=self.group, tg=tg, cfg=self._cfg
            ) as act:
                self._act = act

                self._age_q = anyio.create_queue(10)
                await self.spawn(self._age_killer)

                psutil.cpu_percent(interval=None)
                await self._act.set_value(0)
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
                        load = 100 - psutil.cpu_percent(interval=None)
                        await act.set_value(load)
                        if self.seen_load is not None:
                            pass  # TODO

                        self.node_history += self.name
                        evt = anyio.create_event()
                        await self.spawn(self._run_now, evt)
                        await self._age_q.put(None)
                        await evt.wait()

                    elif isinstance(msg, UntagEvent):
                        await act.set_value(100 - psutil.cpu_percent(interval=None))
                        self.seen_load = 0

                        await self._run_now_task.cancel()
                        # TODO if this is a DetagEvent, kill everything?
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
        for node_ in dropped:
            names.add(node.name)
            del self._nodes[node.name]
        if names:
            await self.state.kill_stale_nodes(names)

    async def _age_killer(self):
        t1 = time.time()
        while self._age_q is not None:
            await self.find_stale_nodes(t1)
            async with anyio.move_on_after(self.max_age) as sc:
                await self._age_q.get()
                t1 = time.time()
                continue
            t2 = time.time()
            if t1 + self.max_age < t2:
                raise NotSelected(self.max_age, t2, t1)
            t1 = t2

    async def _cleanup_nodes(self):
        t = time.time()
        while len(self.node_history) > 1:
            node = self.get_node(self.node_history[-1])
            if t - node.seen < self.max_age:
                break
            assert node.name == self.node_history.pop()
            for j in self.this_root.all_children:
                if j.node == node.name:
                    await j.seems_down()


class RunnerNodeEntry(ClientEntry):
    """
    Sub-node so that a SingleRunnerRoot runs only its local nodes.
    """

    @classmethod
    def child_type(cls, name):
        return RunnerEntry


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
        ``asyncserf.actor`` for details.
    """

    CFG = "singlerunner"

    err = None
    _act = None
    code = None

    def __init__(self, *a, node=None, **kw):
        super().__init__(*a, **kw)
        self.run_name = node

    @classmethod
    def child_type(cls, name):
        return RunnerNodeEntry

    async def set_value(self, val):
        await super().set_value(val)
        try:
            cores = val["cores"]
        except (TypeError, KeyError):
            if self._act is not None:
                await self._act.disable(0)
        else:
            if self.name in cores:
                await self._act.enable(len(cores))
            else:
                await self._act.disable(len(cores))

    async def notify_active(self):
        """
        Notify all running jobs that there's a change in active status
        """
        oac = self._active
        ac = len(self.node_history)
        if ac == 0:
            ac = DetachedState
        elif self.name in self.node_history and ac == 1:
            ac = DetachedState
        elif ac >= self._act.n_nodes:
            ac = CompleteState
        else:
            ac = PartialState

        if oac is not ac:
            self._active = ac
            for n in self.this_root.all_children:
                await n.send_event(ac)

    async def run_starting(self):
        """Hook to set the local root"""
        self.this_root = r = self.get(self.run_name)
        if r is None:
            self.this_root = self.allocate(self.run_name)
        await super().run_starting()

    @property
    def max_age(self):
        """Timeout after which we really should have gotten another ping"""
        return self._act.cycle_time_max * 1.5

    async def _run_actor(self):
        async with anyio.create_task_group() as tg:
            self.tg = tg
            self._age_q = anyio.create_queue(1)

            async with ClientActor(
                self.client, self.name, prefix=self.group, tg=tg, cfg=self._cfg
            ) as act:
                self._act = act
                await tg.spawn(self._age_notifier)
                await self.spawn(self._run_now)
                await act.set_value(0)

                async for msg in act:
                    if isinstance(msg, AuthPingEvent):
                        self.node_history += msg.node
                        await self._age_q.put(None)
                        await self.notify_active()

                pass  # end of actor task

    async def _age_notifier(self):
        while self._age_q is not None:
            flag = False
            async with anyio.move_on_after(self.max_age):
                await self._age_q.get()
                flag = True
            if not flag:
                await self.notify_active()
