"""
This module's job is to run code, resp. to keep it running.


"""

import anyio
from asyncserf.actor import Actor, NodeList
from asyncserf.actor import PingEvent, TagEvent, UntagEvent
from time import monotonic as time
from copy import deepcopy
import psutil
import time
from asyncserf.client import Serf

from .codec import packer, unpacker

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from .exceptions import ServerError
from .client import AttrClientEntry, ClientRoot


class NotSelected(RuntimeError):
    """
    This node has not been selected for a very long time. Something is amiss.
    """

    pass


@asynccontextmanager
async def keep_running(client: Serf, name: str, cfg: dict, evt: anyio.abc.Event = None):
    """
    This task runs all tasks destined to run on this machine, as per the
    configuration.

    Arguments:
      name (str): The (unique) name of this runner.
      cfg (dict): The configuration to use.

    The configuration should look like this:

    Arguments:
      actor (dict): The configuration for the underlying actor.
      prefix (dict): The prefix for the actor. Default "run".
    """
    async with anyio.open_task_group() as tg:
        async with Actor(
            client, name, cfg.get("prefix", "runner"), cfg.get("actor", {}),
            packer=packer, unpacker=unpacker,
        ) as act:
            r = Runner(tg, act, cfg)
            async with r:
                try:
                    yield r
                finally:
                    await tg.cancel_scope.cancel()


class RunnerEntry(AttrClientEntry):
    """
    An entry representing some hopefully-running code.

    The code will run some time after ``target`` has passed.
    On success, it will run again ``repeat`` seconds later (if >0).
    On error, it will run ``delay`` seconds later (if >0), multiplied by 2**backoff.

    Arguments:
      code (tuple): The actual code that should be started.
      data (dict): Some data to go with the code.
      started (float): timestamp when the job was last started
      stopped (float): timestamp when the job last terminated
      delay (float): time before restarting the job on error
      repeat (float): time before restarting on success
      target (float): time the job should be started at
      backoff (int): how often the job terminated with an error
      result: the return value, assuming the job succeeded
      node (str): the node which is running this job, None if not executing

    The code runs with these additional keywords::
      _entry: this object
      _client: the DistKV client connection

    """

    ATTRS = "code data started stopped result node backoff delay repeat target".split()

    started = 0  # timestamp
    stopped = 0  # timestamp
    delay = 100  # timedelta, before restarting
    backoff = 1  # how often a retry failed
    repeat = 0
    target = 0

    node = None  # on which the code is currently running
    code = None  # what to execute
    scope = None  # scope to kill off
    _comment = None  # used for error entries, i.e. mainly Cancel

    def __init__(self, *a, **k):
        super().__init__(*a, **k)

        self._task = None
        self.code = None  # code location
        self.data = {}  # local data

    async def run(self):
        if self.code is None:
            return  # nothing to do here
        try:
            try:
                if self.node is not None:
                    raise RuntimeError("already running on %s", self.node)
                code = self.root.code.follow(*self.code, create=False)
                data = deepcopy(self.data)
                data["_entry"] = self
                data["_client"] = self.root.client

                self.started = time.time()
                self.node = self.root.name

                await self.save(wait=True)
                if self.node != self.root.name:
                    raise RuntimeError("Rudely taken away from us.")

                async with anyio.open_cancel_scope() as sc:
                    self.scope = sc
                    res = code(**data)
                    if code.is_async is not None:
                        res = await res
            finally:
                self.scope = None
                t = time.time()

        except BaseException as exc:
            c, self._comment = self._comment, None
            await self.root.err.record_exc(
                "run", *self._path, exc=exc, data=data, comment=c
            )
            self.backoff += 1
            if self.node == self.root.name:
                self.node = None
        else:
            self.result = res
            self.backoff = 0
            self.node = None
        finally:
            self.stopped = t
            if self.backoff > 0:
                self.retry = t + (1 << self.backoff) * self.delay
            else:
                self.retry = None
            try:
                await self.save()
            except ServerError:
                logger.exception("Could not save")

    async def seems_down(self):
        self.node = None
        await self.save()

    async def set_value(self, val):
        n = self.node
        c = self.code
        await super().set_value(val)

        # Check whether running code needs to be killed off
        if self.scope is None:
            return
        if c != self.code:
            # The code changed.
            self._comment = "Cancel: Code changed"
            await self.scope.cancel()
            return

        if self.node == n:
            # Nothing changed.
            return
        if n == self.root.name:
            # Owch. Our job got taken away from us.
            self._comment = "Cancel: Node set to %r" % (self.node,)
            await self.scope.cancel()
        elif n is not None:
            logger.warning(
                "Runner %s at %r: running but node is %s",
                self.root.name,
                self.subpath,
                n,
            )
        # otherwise this is the change where we took the thing

    async def run_at(self, t: float):
        """Next run at this time.
        """
        self.target = t
        await self.save()

    def should_start(self, t=None):
        """Tell whether this job might want to be started.

        Returns:
          False: No, it's running (or has run and doesn't restart).
          n>0: wait for n seconds before thinking again.
          n<0: should have been started n seconds ago, do something!

        """

        if self.node is not None:
            return False
        if t is None:
            t = time.time()

        if self.target > self.started:
            return self.target - t
        elif self.backoff:
            return self.stopped + self.delay * (1 << self.backoff) - t
        else:
            return False

    def __hash__(self):
        return hash(self.subpath)

    def __eq__(self, other):
        other = getattr(other, "subpath", other)
        return self.name == other

    def __lt__(self, other):
        other = getattr(other, "subpath", other)
        return self.name < other

    @property
    def age(self):
        return time.time() - self.started


class RunnerNode:
    """
    Represents all nodes in this runner group.
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


class RunnerRoot(ClientRoot):
    """
    This class represents the root of a code runner. Its job is to start
    (and periodically restart, if required) the entry points stored under it.

    Config file:

    Arguments:
      path (tuple): the location this entry is stored at. Defaults to
        ``('.distkv', 'run')``.
      name (str): this runner's name. Defaults to the client's name plus
        the name stored in the root node, if any.
      actor (dict): the configuration for the underlying actor. See
        ``asyncserf.actor`` for details.
    """

    CFG = dict(prefix=(".distkv", "run"), start_delay=1)

    ACTOR_CFG = dict(cycle=5, nodes=-1, splits=5)

    _age_killer_task = None
    _run_now_task = None

    err = None
    code = None
    name = None

    @classmethod
    def child_type(cls, name):
        return RunnerEntry

    async def run_starting(self):
        from .errors import ErrorRoot
        from .code import CodeRoot

        self.err = await ErrorRoot.as_handler(self.client)
        self.code = await CodeRoot.as_handler(self.client)

        g = ["run"]
        if "name" in self._cfg:
            g.append(self._cfg["name"])
        if self.value and "name" in self.value:
            g.append(self.value["name"])
        self.group = ".".join(g)
        self.node_history = NodeList(0)
        self._start_delay = self._cfg["start_delay"]

        await super().run_starting()

    async def running(self):
        await self._tg.spawn(self._run_actor)

    def get_node(self, name):
        return RunnerNode(self, name)

    @property
    def name(self):
        """my node name"""
        return self._name

    @property
    def max_age(self):
        """Timeout after which we really should have gotten another go"""
        return self._act.cycle_time_max * (self._act.history_size + 1) * 1.5

    async def _run_actor(self):
        async with anyio.create_task_group() as tg:
            self.tg = tg
            cfg = {}
            cfg.update(type(self).ACTOR_CFG)
            cfg.update(self._cfg.get("actor", {}))
            async with Actor(self.client, self.name, self.group, tg=tg, cfg=cfg) as act:
                self._act = act
                psutil.cpu_percent(interval=None)
                await self._act.set_value(0)
                self.seen_load = None

                async for msg in self._act:
                    if isinstance(msg, PingEvent):
                        await self._act.set_value(
                            100 - psutil.cpu_percent(interval=None)
                        )

                        node = self.get_node(msg.node)
                        node.load = msg.value
                        node.seen = time.time()
                        if self.seen_load is not None:
                            self.seen_load += msg.value
                        self.node_history += node

                    elif isinstance(msg, TagEvent):
                        load = 100 - psutil.cpu_percent(interval=None)
                        await self._act.set_value(load)
                        if self.seen_load is not None:
                            pass  # TODO

                        self.node_history += self.name
                        evt = anyio.create_event()
                        await self.spawn(self._run_now, evt)
                        await self.spawn(self._age_killer)
                        await evt.wait()

                    elif isinstance(msg, UntagEvent):
                        await self._act.set_value(
                            100 - psutil.cpu_percent(interval=None)
                        )
                        self.seen_load = 0

                        await self._run_now_task.cancel()
                        # TODO if this is a DetagEvent, kill everything?

    async def _run_now(self, evt):
        async with anyio.create_task_group() as tg:
            self._run_now_task = tg.cancel_scope
            await evt.set()

            await tg.spawn(self._cleanup_nodes)

            for j in self.all_children:
                if j.should_start():
                    await self.tg.spawn(j.run)
                    await anyio.sleep(self._start_delay)

    async def _age_killer(self):
        if self._age_killer_task is not None:
            await self._age_killer_task.cancel()
        async with anyio.open_cancel_scope() as sc:
            self._age_killer_task = sc
            await anyio.sleep(self.max_age)
        if not sc.cancel_called:
            raise NotSelected(self.max_age, t, time.time())

    async def _cleanup_nodes(self):
        while len(self.node_history) > 1:
            node = self.get_node(self.node_history[-1])
            if node.age < self.max_age:
                break
            assert node.name == self.node_history.pop()
            for j in self.all_children:
                if j.node == node.name:
                    await j.seems_down()
