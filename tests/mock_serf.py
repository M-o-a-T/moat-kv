
try:
    from contextlib import asynccontextmanager, AsyncExitStack
except ImportError:
    from async_generator import asynccontextmanager
    from async_exit_stack import AsyncExitStack
import anyio
import mock
import attr
import copy
import trio
import time
from pprint import pformat
from functools import partial

from distkv.client import open_client
from distkv.default import CFG
from distkv.server import Server
from distkv.codec import unpacker
from distkv.util import attrdict

import logging
logger = logging.getLogger(__name__)

otm = time.time

@asynccontextmanager
async def stdtest(n=1, run=True, client=True, tocks=20, **kw):
    TESTCFG = copy.deepcopy(CFG)
    TESTCFG.server.port = None
    TESTCFG.root="test"

    clock = trio.hazmat.current_clock()
    clock._autojump_threshold = 0.001

    @attr.s
    class S:
        tg = attr.ib()
        serfs = attr.ib(factory=set)
        splits = attr.ib(factory=set)
        s = [] # servers
        c = [] # clients

        async def ready(self, i=None):
            if i is not None:
                await self.s[i].is_ready
                return self.s[i]
            for s in self.s:
                if s is not None:
                    await s.is_ready
            return self.s

        def __iter__(self):
            return iter(self.s)

        @asynccontextmanager
        async def client(self, i:int = 0, args: dict={}):
            """Get a client for the i'th server."""
            await self.s[i].is_serving
            async with open_client(host='localhost', port=st.s[i].port, **args) as c:
                yield c

        def split(self, s):
            assert s not in self.splits
            logger.debug("Split: add %d",s)
            self.splits.add(s)

        def join(self, s):
            logger.debug("Split: join %d",s)
            self.splits.remove(s)

    async def mock_send_ping(self,old):
        assert self._tock < tocks
        await old()

    async def mock_get_host_port(st, node):
        i = int(node.name[node.name.rindex('_')+1:])
        s = st.s[i]
        await s.is_serving
        return ('localhost',s.port)

    def tm():
        try:
            return trio.current_time()
        except RuntimeError:
            return otm()

    async with anyio.create_task_group() as tg:
        st = S(tg)
        async with AsyncExitStack() as ex:
            ex.enter_context(mock.patch("time.time", new=tm))
            ex.enter_context(mock.patch("aioserf.serf_client", new=partial(mock_serf_client,st)))

            for i in range(n):
                name = "test_"+str(i)
                args = kw.get(name, kw.get('args', attrdict()))
                if 'cfg' not in args:
                    args['cfg'] = args.get('cfg',TESTCFG).copy()
                    args['cfg']['serf'] = args['cfg']['serf'].copy()
                    args['cfg']['serf']['i'] = i
                s = Server(name, **args)
                ex.enter_context(mock.patch.object(s, "_send_ping", new=partial(mock_send_ping,s,s._send_ping)))
                ex.enter_context(mock.patch.object(s, "_get_host_port", new=partial(mock_get_host_port,st)))
                st.s.append(s)
            for i in range(n):
                if kw.get("run_"+str(i), run):
                    r = anyio.create_event()
                    await tg.spawn(st.s[i].serve,r)
                    await r.wait()
            try:
                yield st
            finally:
                logger.info("Runtime: %s", clock.current_time())
                await tg.cancel_scope.cancel()
        logger.info("End")

@asynccontextmanager
async def mock_serf_client(master, **cfg):
    async with anyio.create_task_group() as tg:
        ms = MockServ(tg, master, **cfg)
        master.serfs.add(ms)
        try:
            yield ms
        finally:
            master.serfs.remove(ms)

class MockServ:
    def __init__(self, tg, master, **cfg):
        self.cfg = cfg
        self.tg = tg
        self.streams = {}
        self._master = master

    def __hash__(self):
        return id(self)

    def spawn(self, *args, **kw):
        return self.tg.spawn(*args, **kw)

    def stream(self, event_types='*'):
        if ',' in event_types or not event_types.startswith('user:'):
            raise RuntimeError("not supported")
        s = MockSerfStream(self, event_types)
        return s

    async def event(self, typ, payload, coalesce=False):
        logger.debug("SERF:%s: %s", typ, repr(unpacker(payload)))
        typ = typ[typ.index('.')+1:]
        for s in list(self._master.serfs):
            for x in self._master.splits:
                if (s.cfg.get('i',0) < x) != (self.cfg.get('i',0) < x):
                    break
            else:
                s = s.streams.get(typ, None)
                if s is not None:
                    await s.q.put(payload)

class MockSerfStream:
    def __init__(self, serf, typ):
        self.serf = serf
        self.typ = typ[typ.index('.')+1:]

    async def __aenter__(self):
        if self.typ in self.serf.streams:
            raise RuntimeError("Only one listener per event type allowed")
        self.q = anyio.create_queue(100)
        self.serf.streams[self.typ] = self
        return self

    async def __aexit__(self, *tb):
        del self.serf.streams[self.typ]
        del self.q

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.q.get()
        return attrdict(payload=res)

