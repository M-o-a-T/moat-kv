import pytest
import trio
import anyio
import time

from .mock_serf import stdtest

from .run import run
from functools import partial

from distkv.client import ServerError
from distkv.util import PathLongener

from distkv.code import CodeRoot
from distkv.runner import RunnerRoot
from distkv.errors import ErrorRoot
import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_83_run(autojump_clock):
    from distkv import runner

    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            r = await RunnerRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            c._test_evt = anyio.create_event()
            await cr.add(
                "forty",
                "two",
                code="""\
                import trio
                c=kw['_client']
                await c._test_evt.set()
                await trio.sleep(10)
                return 42
                """,
                is_async=True,
            )
            ru = r.follow("foo", "test")
            ru.code = ("forty", "two")
            await ru.run_at(time.time())
            logger.info("Start sleep")
            with trio.fail_after(60):
                await c._test_evt.wait()
            await run("client", "-h", h, "-p", p, "get", "-ryd_", do_stdout=False)
            await trio.sleep(11)

            logger.info("End sleep")

            r = await run("client", "-h", h, "-p", p, "get")
            assert r.stdout == "123\n"

            assert ru.started > 0
            assert ru.stopped > 0
            assert ru.backoff == 0
            assert ru.result == 42
