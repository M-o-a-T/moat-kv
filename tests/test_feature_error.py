import pytest
import trio

from .mock_serf import stdtest
from .run import run

# from .run import run
# from functools import partial

from distkv.auth import loader
from distkv.client import ServerError
from distkv.util import PathLongener

from distkv.errors import ErrorRoot
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
async def test_81_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            async with st.client() as cx:
                e = await ErrorRoot.as_handler(c)
                ex = await ErrorRoot.as_handler(cx)
                try:
                    1 / 0
                except Exception as exc:
                    await ex.record_error("tester", "here", "or", "there", exc=exc)
                await trio.sleep(1)
                n = 0
                for err in e.all_errors("tester"):
                    n += 1
                    await err.resolve()
                assert n == 1
                await trio.sleep(1)

@pytest.mark.trio
async def test_82_many(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as cx, st.client() as cy, st.client() as cz:
                ex = await ErrorRoot.as_handler(cx, name="a1")
                ey = await ErrorRoot.as_handler(cy, name="a2")
                ez = await ErrorRoot.as_handler(cz, name="a3")

                async def err(e):
                    await e.record_error("tester", "dup", message="Owchie at {node}", data={'node':e.name})
                async with trio.open_nursery() as tg:
                    tg.start_soon(err, ex)
                    tg.start_soon(err, ey)
                    tg.start_soon(err, ez)
                    await trio.sleep(2)

                for h, p, *_ in s.ports:
                    if h[0] != ":":
                        break
                r = await run("client", "-m", "-h", h, "-p", p, "data", "get", "-rd_", do_stdout=False)

                n = 0
                for err in ex.all_errors("tester"):
                    n += 1
                    assert len(list(err)) == 3
                    for k in err:
                        assert k._name in {"a1","a2","a3"}, k
                    await err.resolve()
                assert n == 1
                await trio.sleep(1)
