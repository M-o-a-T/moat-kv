import pytest
import trio
import anyio
import time

from distkv.mock.mqtt import stdtest

from distkv.code import CodeRoot
from distkv.runner import SingleRunnerRoot
from distkv.errors import ErrorRoot
from distkv.util import P

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_83_run(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=100) as st:
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            r = await SingleRunnerRoot.as_handler(c, subpath=(), code=cr)
            c._test_evt = anyio.create_event()
            await cr.add(
                P("forty.two"),
                code="""\
                import trio
                c=_client
                await c._test_evt.set()
                await trio.sleep(10)
                return 42
                """,
                is_async=True,
            )
            ru = r.follow(P("testnode.foo.test"), create=True)
            ru.code = P("forty.two")
            await ru.run_at(time.time())
            logger.info("Start sleep")
            with trio.fail_after(60):
                await c._test_evt.wait()
            await st.run("data get -rd_ :", do_stdout=False)
            await trio.sleep(11)

            logger.info("End sleep")

            r = await st.run("data get :")
            assert r.stdout == "123\n"

            rs = ru.state
            assert rs.started > 0
            assert rs.stopped > 0
            assert rs.backoff == 0
            assert rs.result == 42
