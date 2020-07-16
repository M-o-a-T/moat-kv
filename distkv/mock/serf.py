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
from functools import partial
from asyncscope import main_scope

from distkv.default import CFG
from distkv.server import Server
from distkv.codec import unpacker
from distkv.util import attrdict, combine_dict, NotGiven, ValueEvent
from distkv.mock import S as _S
from asyncserf.stream import SerfEvent
from anyio import create_queue

import logging

logger = logging.getLogger(__name__)

otm = time.time


@asynccontextmanager
async def stdtest(n=1, run=True, ssl=False, tocks=20, **kw):
    C_OUT = CFG.get("_stdout", NotGiven)
    if C_OUT is not NotGiven:
        del CFG["_stdout"]
    TESTCFG = copy.deepcopy(CFG)
    TESTCFG.server.port = None
    TESTCFG.server.backend = "serf"
    TESTCFG.root = "test"
    if C_OUT is not NotGiven:
        CFG["_stdout"] = C_OUT
        TESTCFG["_stdout"] = C_OUT

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
        server_ctx = client_ctx = False

    clock = trio.lowlevel.current_clock()
    clock.autojump_threshold = 0.0
    # clock.rate = 5

    @attr.s
    class S(_S):
        splits = attr.ib(factory=set)

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

    async def mock_set_tock(self, old):
        assert self._tock < tocks, "Test didn't terminate. Limit:" + str(tocks)
        await old()

    async with main_scope("_distkv_test_serf") as scp:
        tg = scp._tg
        st = S(tg, client_ctx)
        async with AsyncExitStack() as ex:
            st.ex = ex  # pylint: disable=attribute-defined-outside-init
            ex.enter_context(mock.patch("time.time", new=tm))
            ex.enter_context(mock.patch("time.monotonic", new=tm))
            logging._startTime = tm()

            ex.enter_context(
                mock.patch("asyncserf.serf_client", new=partial(mock_serf_client, st))
            )

            for i in range(n):
                name = "test_" + str(i)
                args = kw.get(name, kw.get("args", attrdict()))
                args["cfg"] = combine_dict(
                    args.get("cfg", {}),
                    {
                        "connect": {"ssl": client_ctx},
                        "server": {
                            "bind_default": {
                                "host": "127.0.0.1",
                                "port": i + 50120,
                                "ssl": server_ctx,
                            },
                            "serf": {"i": i},
                        },
                    },
                    TESTCFG,
                )
                s = Server(name, **args)
                ex.enter_context(
                    mock.patch.object(s, "_set_tock", new=partial(mock_set_tock, s, s._set_tock))
                )
                ex.enter_context(
                    mock.patch.object(s, "_get_host_port", new=partial(mock_get_host_port, st))
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
                async with anyio.fail_after(2, shield=True):
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

    async def event(self, name, payload, coalesce=True):
        try:
            logger.debug("SERF:%s: %r", name, unpacker(payload))
        except Exception:
            logger.debug("SERF:%s: %r (raw)", name, payload)
        assert not coalesce, "'coalesce' must be cleared!"

        i_self = self.cfg.get("i", 0)
        for s in list(self._master.serfs):
            i_s = s.cfg.get("i", 0)
            for x in self._master.splits:
                if (i_s < x) != (i_self < x):
                    break
            else:
                n = tuple(name.split("."))
                while n:
                    sl = s.streams.get(n, ())
                    for sn in sl:
                        await sn.q.put((name, payload))
                    n = n[:-1]

    def stream(self, typ):
        """compat for supporting asyncactor"""
        if not typ.startswith("user:"):
            raise RuntimeError("not supported")
        typ = typ[5:]
        return self.serf_mon(typ)

    def serf_mon(self, typ):
        if "," in typ:
            raise RuntimeError("not supported")
        s = MockSerfStream(self, "user:" + typ)
        return s

    async def serf_send(self, typ, payload):
        """compat for supporting asyncactor"""
        return await self.event(typ, payload)


class MockSerfStream:
    q = None

    def __init__(self, serf, typ):
        self.serf = serf
        assert typ.startswith("user:")
        self.typ = tuple(typ[5:].split("."))

    async def __aenter__(self):
        self.q = create_queue(100)
        self.serf.streams.setdefault(self.typ, []).append(self)
        return self

    async def __aexit__(self, *tb):
        self.serf.streams[self.typ].remove(self)
        del self.q

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.q.get()
        evt = SerfEvent(self)
        evt.topic, evt.payload = res
        return evt
