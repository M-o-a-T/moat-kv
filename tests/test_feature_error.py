import pytest
import trio

from .mock_serf import stdtest

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
                    await ex.record_exc("tester", "here", "or", "there", exc=exc)
                await trio.sleep(1)
                n = 0
                for err in e.all_errors("tester"):
                    n += 1
                    await err.resolve()
                assert n == 1
                await trio.sleep(1)
