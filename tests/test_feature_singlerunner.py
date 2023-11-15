import logging
import time

import anyio
import pytest
import trio
from moat.util import P

from moat.kv.code import CodeRoot
from moat.kv.errors import ErrorRoot
from moat.kv.mock.mqtt import stdtest
from moat.kv.runner import SingleRunnerRoot

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_83_run(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=200) as st:
        assert st is not None
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            r = await SingleRunnerRoot.as_handler(c, subpath=(), code=cr)
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
            ru = r.follow(P("testnode.foo.test"), create=True)
            ru.code = P("forty.two")
            await ru.run_at(time.time())
            logger.info("Start sleep")
            try:
                with trio.fail_after(200):
                    await c._test_evt.wait()
            finally:
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
