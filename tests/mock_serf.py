try:
    from contextlib import asynccontextmanager, AsyncExitStack
except ImportError:
    from async_generator import asynccontextmanager
    from async_exit_stack import AsyncExitStack
import trio
import anyio
import mock
import attr
import copy
import time
import socket
from functools import partial

from distkv.client import open_client
from distkv.default import CFG
from distkv.exceptions import CancelledError
from distkv.server import Server
from distkv.codec import unpacker
from distkv.util import attrdict
from asyncserf.util import ValueEvent
from asyncserf.stream import SerfEvent
from anyio import create_queue

import logging

logger = logging.getLogger(__name__)

otm = time.time


@asynccontextmanager
async def stdtest(n=1, run=True, client=True, ssl=False, tocks=20, **kw):
    TESTCFG = copy.deepcopy(CFG)
    TESTCFG.server.port = None
    TESTCFG.root = "test"

    if ssl:
        import ssl
        import trustme

        ca = trustme.CA()
        cert = ca.issue_server_cert(u"127.0.0.1")
        server_ctx = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        client_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ca.configure_trust(client_ctx)
        cert.configure_cert(server_ctx)
    else:
        server_ctx = client_ctx = None

    clock = trio.hazmat.current_clock()
    clock.autojump_threshold = 0.1

    @attr.s
    class S:
        tg = attr.ib()
        serfs = attr.ib(factory=set)
        splits = attr.ib(factory=set)
        s = []  # servers
        c = []  # clients

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
        async def client(self, i: int = 0, **kv):
            """Get a client for the i'th server."""
            await self.s[i].is_serving
            for host, port, *_ in st.s[i].ports:
                if host[0] == ":":
                    continue
                try:
                    async with open_client(
                        host=host, port=port, ssl=client_ctx, **kv
                    ) as c:
                        yield c
                        return
                except socket.gaierror:
                    pass
            raise RuntimeError("Duh? no connection")

        def split(self, s):
            assert s not in self.splits
            logger.debug("Split: add %d", s)
            self.splits.add(s)

        def join(self, s):
            logger.debug("Split: join %d", s)
            self.splits.remove(s)

    async def mock_get_host_port(st, host):
        i = int(host[host.rindex("_") + 1 :])  # noqa: E203
        s = st.s[i]
        await s.is_serving
        for host, port, *_ in s.ports:
            if host[0] != ":":
                return host, port

    def tm():
        try:
            return trio.current_time()
        except RuntimeError:
            return otm()

    async with anyio.create_task_group() as tg:
        st = S(tg)
        async with AsyncExitStack() as ex:
            st.ex = ex
            ex.enter_context(mock.patch("time.time", new=tm))
            logging._startTime = tm()

            ex.enter_context(
                mock.patch("asyncserf.serf_client", new=partial(mock_serf_client, st))
            )

            for i in range(n):
                name = "test_" + str(i)
                args = kw.get(name, kw.get("args", attrdict()))
                if "cfg" not in args:
                    args["cfg"] = args.get("cfg", TESTCFG).copy()
                    args["cfg"]["serf"] = args["cfg"]["serf"].copy()
                    args["cfg"]["serf"]["i"] = i
                    if server_ctx:
                        args["cfg"]["server"] = args["cfg"]["server"].copy()
                        args["cfg"]["server"]["ssl"] = server_ctx
                s = Server(name, **args)
                # ex.enter_context(
                #     mock.patch.object(
                #         s, "_send_ping", new=partial(mock_send_ping, s, s._send_ping)
                #     )
                # )
                ex.enter_context(
                    mock.patch.object(
                        s, "_get_host_port", new=partial(mock_get_host_port, st)
                    )
                )
                st.s.append(s)

            evts = []
            for i in range(n):
                if kw.get("run_" + str(i), run):
                    evt = anyio.create_event()
                    await tg.spawn(partial(st.s[i].serve, ready_evt=evt))
                    evts.append(evt)
            for e in evts:
                await e.wait()
            try:
                yield st
            finally:
                logger.info("Runtime: %s", clock.current_time())
                await tg.cancel_scope.cancel()
        logger.info("End")
        pass  # unwinding ex:AsyncExitStack


@asynccontextmanager
async def mock_serf_client(master, **cfg):
    async with anyio.create_task_group() as tg:
        ms = MockServ(tg, master, **cfg)
        master.serfs.add(ms)
        try:
            yield ms
        finally:
            master.serfs.remove(ms)
        pass  # terminating mock_serf_client nursery


class MockServ:
    def __init__(self, tg, master, **cfg):
        self.cfg = cfg
        self._tg = tg
        self.streams = {}
        self._master = master

    def __hash__(self):
        return id(self)

    async def spawn(self, fn, *args, **kw):
        async def run(evt):
            async with anyio.open_cancel_scope() as sc:
                await evt.set(sc)
                await fn(*args, **kw)

        evt = ValueEvent()
        await self._tg.spawn(run, evt)
        return await evt.get()

    def stream(self, event_types="*"):
        if "," in event_types or not event_types.startswith("user:"):
            raise RuntimeError("not supported")
        s = MockSerfStream(self, event_types)
        return s

    async def event(self, typ, payload, coalesce=False):
        try:
            logger.debug("SERF:%s: %r", typ, unpacker(payload))
        except Exception:
            logger.debug("SERF:%s: %r", typ, payload)

        for s in list(self._master.serfs):
            for x in self._master.splits:
                if (s.cfg.get("i", 0) < x) != (self.cfg.get("i", 0) < x):
                    break
            else:
                sl = s.streams.get(typ, None)
                if sl is not None:
                    for s in sl:
                        await s.q.put(payload)

    def stream(self, typ):
        """compat for supporting asyncserf.actor"""
        if not typ.startswith('user:'):
            raise RuntimeError("not supported")
        typ = typ[5:]
        return self.serf_mon(typ)

    def serf_mon(self, typ):
        if "," in typ:
            raise RuntimeError("not supported")
        s = MockSerfStream(self, "user:" + typ)
        return s

    async def event(self, typ, payload):
        """compat for supporting asyncserf.actor"""
        return await self.serf_send(typ data=payload)

    async def serf_send(self, typ, data):
        # logger.debug("SERF:%s: %r", typ, data)

        for s in list(self._master.serfs):
            for x in self._master.splits:
                if (s.cfg.get("i", 0) < x) != (self.cfg.get("i", 0) < x):
                    break
            else:
                sl = s.streams.get(typ, None)
                if sl is not None:
                    for s in sl:
                        await s.q.put(data)


class MockSerfStream:
    def __init__(self, serf, typ):
        self.serf = serf
        assert typ.startswith("user:")
        self.typ = typ[5:]

    async def __aenter__(self):
        logger.debug("SERF:MON START:%s", self.typ)
        self.q = create_queue(100)
        self.serf.streams.setdefault(self.typ, []).append(self)
        return self

    async def __aexit__(self, *tb):
        self.serf.streams[self.typ].remove(self)
        logger.debug("SERF:MON END:%s", self.typ)
        del self.q

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.q.get()
        evt = SerfEvent(self)
        evt.payload=res
        return evt

