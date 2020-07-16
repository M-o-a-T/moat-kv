import pytest
import trio

from distkv.mock.mqtt import stdtest

from distkv.util import PathLongener, P

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
async def test_81_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=40) as st:
        async with st.client() as c:
            e = await ErrorRoot.as_handler(c)
            async with st.client() as cx:
                ex = await ErrorRoot.as_handler(cx)
                try:
                    1 / 0
                except Exception as exc:
                    await ex.record_error("tester", P("here.or.there"), exc=exc)
                await trio.sleep(1)
                n = 0
            for err in e.all_errors("tester"):
                n += 1
                await err.resolve()
            assert n == 1
            await trio.sleep(1)


@pytest.mark.trio
@pytest.mark.xfail
async def test_82_many(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=80) as st:
        async with st.client() as cx, st.client() as cy, st.client() as cz:
            ex = await ErrorRoot.as_handler(cx, name="a1")
            ey = await ErrorRoot.as_handler(cy, name="a2")
            ez = await ErrorRoot.as_handler(cz, name="a3")

            async def err(e):
                with trio.CancelScope(shield=True):
                    await e.record_error(
                        "tester", P("dup"), message="Owchie at {node}", data={"node": e.name}
                    )

            async with trio.open_nursery() as tg:
                tg.start_soon(err, ex)
                tg.start_soon(err, ey)
                tg.start_soon(err, ez)
                await trio.sleep(2)

            await st.run("data get -rd_", do_stdout=False)
            await trio.sleep(2)

            n = 0
            for err in ex.all_errors("tester"):
                n += 1
                logger.warning("DEL ASSERT %d", n)
                assert len(list(err)) == 3, list(err)
                for k in err:
                    assert k._name in {"a1", "a2", "a3"}, k
                await err.resolve()
            assert n == 1
            await trio.sleep(1)
