import pytest
import trio

from .mock_serf import stdtest

# from .run import run
# from functools import partial

from distkv.auth import loader
from distkv.client import ServerError
from distkv.util import PathLongener

import logging

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
async def test_71_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            await c._request(
                "set_internal",
                path=("codec", "int"),
                value={
                    "in": [("1", 1), ("2", 2), ("3", 3)],
                    "out": [(1, "1"), (2, "2"), (-3, "-3")],
                    "encode": "return str(value)",
                    "decode": "assert isinstance(value,str); return int(value)",
                },
            )
            await c._request(
                "set_internal",
                path=("conv", "foo", "inty", "#"),
                value={"codec": "int"},
            )
            um = loader("_test", "user", make=True, server=False)
            u = um.build({"name": "std"})
            await u.send(c)
            u = um.build({"name": "con", "aux": {"conv": "foo"}})
            await u.send(c)
            await c._request("set_auth_typ", typ="_test")

        recv = []
        um = loader("_test", "user", make=False, server=False)

        async def mon(evt):
            async with st.client(auth=um.build({"name": "std"})) as c:
                async with c._stream("watch", path=("inty",)) as q:
                    evt.set()
                    pl = PathLongener(("inty",))
                    async for m in q:
                        pl(m)
                        del m["tock"]
                        del m["seq"]
                        recv.append(m)

        evt = trio.Event()
        await s.spawn(mon, evt)
        await evt.wait()
        async with st.client(auth=um.build({"name": "con"})) as c:
            await c.set("inty", "ten", value="10")
            with pytest.raises(ServerError):
                await c.set("inty", "nope", value=11)
            await c.set("inty", "yep", "yepyepyep", value="13")
            with pytest.raises(ServerError):
                await c.set("inty", "nope", "nopenope", value=12)
            await c.set("inty", "yep", "yepyepyep", "yep", value="99")
            await c.set("inty", value="hello")

            r = await c.get("inty")
            assert r.value == "hello"
            r = await c.get("inty", "ten")
            assert r.value == "10"
            r = await c.get("inty", "yep", "yepyepyep")
            assert r.value == "13"

            # run_c = partial(run, "-D", "client", "-h", s.ports[0][0], "-p", s.ports[0][1])
            # await run_c("-a","_test name=std", "get", "-yrd_", do_stdout=False)

        assert recv == [
            {"path": ("inty", "ten"), "value": 10},
            {"path": ("inty", "yep", "yepyepyep"), "value": 13},
            {"path": ("inty", "yep", "yepyepyep", "yep"), "value": 99},
            {"path": ("inty",), "value": "hello"},
        ]
