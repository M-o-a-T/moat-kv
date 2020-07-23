try:
    from contextlib import asynccontextmanager, AsyncExitStack
except ImportError:
    from async_generator import asynccontextmanager
    from async_exit_stack import AsyncExitStack
import os
import trio
import anyio
import mock
import copy
import time
from functools import partial
from asyncscope import main_scope

from distkv.default import CFG
from distkv.server import Server
from distkv.util import attrdict, combine_dict, NotGiven
from distkv.mock import S
from distmqtt.broker import create_broker

import logging

logger = logging.getLogger(__name__)

otm = time.time

PORT = 40000 + (os.getpid() + 10) % 10000

broker_cfg = {
    "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{PORT}"}},
    "timeout-disconnect-delay": 2,
    "auth": {"allow-anonymous": True, "password-file": None},
}

URI = f"mqtt://127.0.0.1:{PORT}/"


@asynccontextmanager
async def stdtest(n=1, run=True, ssl=False, tocks=20, **kw):
    C_OUT = CFG.get("_stdout", NotGiven)
    if C_OUT is not NotGiven:
        del CFG["_stdout"]
    TESTCFG = copy.deepcopy(CFG)
    TESTCFG.server.port = None
    TESTCFG.root = "test"
    if C_OUT is not NotGiven:
        CFG["_stdout"] = C_OUT
        TESTCFG["_stdout"] = C_OUT

    if ssl:
        import ssl
        import trustme

        ca = trustme.CA()
        cert = ca.issue_server_cert("127.0.0.1")
        server_ctx = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH)
        client_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ca.configure_trust(client_ctx)
        cert.configure_cert(server_ctx)
    else:
        server_ctx = client_ctx = False

    clock = trio.lowlevel.current_clock()
    # clock.autojump_threshold = 0.1
    # clock.rate = 5

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

    async with main_scope("_distkv_test_mqtt") as scp:
        tg = scp._tg
        st = S(tg, client_ctx)
        async with AsyncExitStack() as ex:
            st.ex = ex  # pylint: disable=attribute-defined-outside-init
            ex.enter_context(mock.patch("time.time", new=tm))
            ex.enter_context(mock.patch("time.monotonic", new=tm))
            logging._startTime = tm()
            await ex.enter_async_context(create_broker(config=broker_cfg))

            args_def = kw.get("args", attrdict())
            for i in range(n):
                name = "test_" + str(i)
                args = kw.get(name, args_def)
                args = combine_dict(
                    args,
                    args_def,
                    {
                        "cfg": {
                            "connect": {"ssl": client_ctx},
                            "server": {
                                "bind_default": {
                                    "host": "127.0.0.1",
                                    "port": i + PORT + 1,
                                    "ssl": server_ctx,
                                },
                                "backend": "mqtt",
                                "mqtt": {"uri": URI},
                            },
                        }
                    },
                    {"cfg": TESTCFG},
                )
                args_def.pop("init", None)
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
        pass  # unwinding AsyncExitStack
