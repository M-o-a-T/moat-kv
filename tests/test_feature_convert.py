import logging

import pytest
import trio
from asyncscope import scope
from moat.src.test import raises
from moat.util import P, PathLongener

from moat.kv.auth import loader
from moat.kv.client import ServerError
from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


async def collect(i, path=()):
    res = []
    pl = PathLongener(path)
    async for r in i:
        r.pop("tock", 0)
        r.pop("seq", 0)
        pl(r)
        res.append(r)
    return res


@pytest.mark.trio
async def test_71_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=100) as st:
        assert st is not None
        async with st.client() as c:
            await c._request(
                "set_internal",
                path=P("codec.int"),
                value={
                    "in": [("1", 1), ("2", 2), ("3", 3)],
                    "out": [(1, "1"), (2, "2"), (-3, "-3")],
                    "encode": "return str(value)",
                    "decode": "assert isinstance(value,str); return int(value)",
                },
            )
            await c._request(
                "set_internal", path=P("conv.foo.inty.#"), value={"codec": "int"}
            )
            um = loader("_test", "user", make=True, server=False)
            u = um.build({"name": "std"})
            await u.send(c)
            u = um.build({"name": "con"})
            await c._request(
                "set_internal",
                path=P("auth._test.user.con.conv"),
                value=dict(key="foo"),
                iter=False,
            )
            await u.send(c)
            await c._request("set_auth_typ", typ="_test")

        recv = []
        um = loader("_test", "user", make=False, server=False)

        async def mon(evt):
            async with st.client(auth=um.build({"name": "std"})) as c:
                async with c._stream("watch", path=P("inty")) as q:
                    evt.set()
                    pl = PathLongener(P("inty"))
                    async for m in q:
                        pl(m)
                        del m["tock"]
                        del m["seq"]
                        recv.append(m)

        evt = trio.Event()
        await scope.spawn(mon, evt)
        await evt.wait()
        async with st.client(auth=um.build({"name": "con"})) as c:
            await c.set(P("inty.ten"), value="10")
            with raises(ServerError):
                await c.set(P("inty.nope"), value=11)
            await c.set(P("inty.yep.yepyepyep"), value="13")
            with raises(ServerError):
                await c.set(P("inty.nope.nopenope"), value=12)
            await c.set(P("inty.yep.yepyepyep.yep"), value="99")
            await c.set(P("inty"), value="hello")

            r = await c.get(P("inty"))
            assert r.value == "hello"
            r = await c.get(P("inty.ten"))
            assert r.value == "10"
            r = await c.get(P("inty.yep.yepyepyep"))
            assert r.value == "13"

            # run_c = partial(run, "-D", "kv", "-h", s.ports[0][0], "-p", s.ports[0][1])
            # await run_c("-a","_test name=std", "get", "-rd_", do_stdout=False)

        assert recv == [
            {"path": P("inty.ten"), "value": 10},
            {"path": P("inty.yep.yepyepyep"), "value": 13},
            {"path": P("inty.yep.yepyepyep.yep"), "value": 99},
            {"path": P("inty"), "value": "hello"},
        ]
