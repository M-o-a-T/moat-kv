import logging
import time

import anyio
import pytest
import trio
from moat.util import P

from moat.kv.code import CodeRoot
from moat.kv.errors import ErrorRoot
from moat.kv.mock.mqtt import stdtest
from moat.kv.runner import AllRunnerRoot

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_83_run(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=200) as st:
        assert st is not None
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            r = await AllRunnerRoot.as_handler(c, subpath=(), code=cr)
            c._test_evt = anyio.Event()
            await cr.add(
                P("forty.two"),
                code="""\
                import trio
                c=_client
                c._test_evt.set()
                await trio.sleep(10)
                return 42
                """,
                is_async=True,
            )
            ru = r.follow(P("foo.test"), create=True)
            ru.code = P("forty.two")
            await ru.run_at(time.time() + 1)
            logger.info("Start sleep")
            try:
                with trio.fail_after(200):
                    await c._test_evt.wait()
            finally:
                # this might block due to previous error
                with anyio.fail_after(2):
                    await st.run("data : get -rd_", do_stdout=False)
            await trio.sleep(11)

            logger.info("End sleep")

            r = await st.run("data :")
            assert r.stdout == "123\n"

            rs = ru.state
            assert rs.started > 0
            assert rs.stopped > 0
            assert rs.backoff == 0
            assert rs.result == 42
