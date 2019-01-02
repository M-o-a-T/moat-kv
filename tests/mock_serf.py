
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
from pprint import pformat
from functools import partial

from distkv.client import open_client
from distkv.default import CFG
from distkv.server import Server
from distkv.codec import unpacker

import logging
logger = logging.getLogger(__name__)

_serfs = set()

@asynccontextmanager
async def stdtest(n=1, run=True, client=True, tocks=20, **kw):
    TESTCFG = copy.deepcopy(CFG)
    TESTCFG.server.port = None

    @attr.s
    class S:
        tg = attr.ib()
        ex = attr.ib()
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

        async def client(self, i:int = 0, args: dict={}):
            """Get a client for the i'th server."""
            await self.s[i].is_serving
            if 'cfg' not in args:
                cfg = TESTCFG
            c = await st.ex.enter_async_context(open_client(host='localhost', port=st.s[i].port))
            return c

    async def mock_send_ping(self,old):
        assert self._tock < tocks
        await old()

    async with anyio.create_task_group() as tg:
        async with AsyncExitStack() as ex:
            ex.enter_context(mock.patch("time.time", new=trio.current_time))
            ex.enter_context(mock.patch("aioserf.serf_client", new=mock_serf_client))

            st = S(tg,ex)
            for i in range(n):
                name = "test_"+str(i)
                args = kw.get(name, kw.get('args', {}))
                if 'cfg' not in args:
                    args['cfg'] = TESTCFG
                s = Server(name, **args)
                pi = ex.enter_context(mock.patch.object(s, "_send_ping", new=partial(mock_send_ping,s,s._send_ping)))
                st.s.append(s)
            for i in range(n):
                if kw.get("run_"+str(i), run):
                    r = anyio.create_event()
                    await tg.spawn(st.s[i].serve,r)
                    await r.wait()
            try:
                yield st
            finally:
                await tg.cancel_scope.cancel()

@asynccontextmanager
async def mock_serf_client(**cfg):
    async with anyio.create_task_group() as tg:
        ms = MockServ(tg, **cfg)
        _serfs.add(ms)
        try:
            yield ms
        finally:
            _serfs.remove(ms)

class MockServ:
    def __init__(self, tg, **cfg):
        self.cfg = cfg
        self.tg = tg
        self.streams = {}

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
        logger.debug("SERF:%s: %s", typ, pformat(unpacker(payload)))
        for s in list(_serfs):
            s = s.streams.get(typ, None)
            if s is not None:
                await s.q.put(payload)

class MockSerfStream:
    def __init__(self, serf, typ):
        self.serf = serf
        self.typ = typ

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
        return await self.q.get()

