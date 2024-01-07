import copy
import logging
import os
import time
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial

import anyio
import mock
import trio
from asyncscope import main_scope, scope
from moat.mqtt.broker import create_broker
from moat.util import NotGiven, attrdict, combine_dict

from moat.kv.mock import S
from moat.kv.server import Server

from . import CFG

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
    TESTCFG = copy.deepcopy(CFG["kv"])
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
    try:
        clock.autojump_threshold = 0.02  # networking
    except Exception:
        pass  # test doesn't have autojump_clock fixture

    async def mock_get_host_port(st, host):
        i = int(host[host.rindex("_") + 1 :])  # noqa: E203
        s = st.s[i]
        await s.is_serving
        for host, port, *_ in s.ports:
            if host == "::" or host[0] != ":":
                return host, port

    def tm():
        try:
            return trio.current_time()
        except RuntimeError:
            return otm()

    async def mock_set_tock(self, old):
        assert self._tock < tocks, "Test didn't terminate. Limit:" + str(tocks)
        await old()

    done = False
    async with main_scope("moat.kv.test.mqtt") as scp:
        tg = scp._tg
        st = S(tg, client_ctx)
        async with AsyncExitStack() as ex:
            st.ex = ex  # pylint: disable=attribute-defined-outside-init
            ex.enter_context(mock.patch("time.time", new=tm))
            ex.enter_context(mock.patch("time.monotonic", new=tm))
            logging._startTime = tm()

            async def run_broker(cfg):
                async with create_broker(config=cfg) as srv:
                    # NB: some services use "async with await …"
                    scope.register(srv)
                    await scope.no_more_dependents()

            async def with_broker(s, *a, **k):
                await scope.service("moat.mqtt.broker", run_broker, broker_cfg)
                s._scope = scope.get()
                return await s._scoped_serve(*a, **k)

            args_def = kw.get("args", attrdict())
            for i in range(n):
                name = "test_" + str(i)
                args = kw.get(name, args_def)
                args = combine_dict(
                    args,
                    args_def,
                    {
                        "cfg": {
                            "conn": {"ssl": client_ctx},
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
                    mock.patch.object(
                        s, "_set_tock", new=partial(mock_set_tock, s, s._set_tock)
                    )
                )
                ex.enter_context(
                    mock.patch.object(
                        s, "_get_host_port", new=partial(mock_get_host_port, st)
                    )
                )
                st.s.append(s)

            evts = []
            for i in range(n):
                if kw.get(f"run_{i}", run):
                    evt = anyio.Event()
                    await scp.spawn_service(with_broker, st.s[i], ready_evt=evt)
                    evts.append(evt)
                else:
                    setattr(
                        st, f"run_{i}", partial(scp.spawn_service, with_broker, st.s[i])
                    )

            for e in evts:
                await e.wait()
            try:
                done = True
                yield st
            finally:
                with anyio.fail_after(2, shield=True):
                    logger.info("Runtime: %s", clock.current_time())
                    tg.cancel_scope.cancel()
    if not done:
        yield None
