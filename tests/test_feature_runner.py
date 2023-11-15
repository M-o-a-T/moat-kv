import logging
import time

import anyio
import pytest
import trio
from moat.util import P

from moat.kv.code import CodeRoot
from moat.kv.errors import ErrorRoot
from moat.kv.mock.mqtt import stdtest
from moat.kv.runner import AnyRunnerRoot

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_83_run(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=200) as st:
        assert st is not None
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            r = await AnyRunnerRoot.as_handler(c, subpath=(), code=cr)
            c._test_evt = anyio.Event()
            await cr.add(
                P("forty.two"),
                code="""\
                c=_client
                s=_self
                c._test_evt.set()
                await s.setup_done()
                await s.watch(_P("test:4242"))
                async for msg in _info:
                    if isinstance(msg, _cls.TimerMsg):
                        return 42
                    elif isinstance(msg, _cls.ChangeMsg) and msg.msg.value == 42:
                        await s.timer(10)
                """,
                is_async=True,
            )
            ru = r.follow(P("foo.test"), create=True)
            ru.code = P("forty.two")
            await ru.run_at(time.time() + 1)
            logger.info("Start sleep")
            rs = ru.state
            with trio.fail_after(300):
                await c._test_evt.wait()
                await c.set(P("test:4242"), value=13)
                await trio.sleep(2)
                assert not rs.stopped
                await c.set(P("test:4242"), value=42)
                await trio.sleep(2)
                assert not rs.stopped
                await trio.sleep(10)
                await st.run("data : get -rd_", do_stdout=False)
                assert rs.stopped
                pass  # end

            await trio.sleep(11)

            logger.info("End sleep")

            assert rs.started > 0
            assert rs.stopped > 0
            assert rs.backoff == 0
            assert rs.result == 42


@pytest.mark.trio
async def test_84_mqtt(autojump_clock):  # pylint: disable=unused-argument
    autojump_clock.autojump_threshold = 0.01
    async with stdtest(args={"init": 123}, tocks=200) as st:
        assert st is not None
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            r = await AnyRunnerRoot.as_handler(c, subpath=(), code=cr)
            c._test_evt = anyio.Event()
            await cr.add(
                P("forty.three"),
                code="""\
                c=_client
                s=_self
                c._test_evt.set()
                await s.setup_done()
                await s.monitor(_P("test.abcd"))
                async for msg in _info:
                    if isinstance(msg, _cls.TimerMsg):
                        return 43
                    elif isinstance(msg, _cls.MQTTmsg) and msg.value == 41:
                        await s.timer(10)
                """,
                is_async=True,
            )
            ru = r.follow(P("foo.test"), create=True)
            ru.code = P("forty.three")
            await ru.run_at(time.time() + 1)
            logger.info("Start sleep")
            rs = ru.state
            try:
                with trio.fail_after(200):
                    await c._test_evt.wait()
                    await c.msg_send(P("test.abcd"), data=13)
                    await trio.sleep(2)
                    assert not rs.stopped
                    await c.msg_send(P("test.abcd"), data=41)
                    await trio.sleep(2)
                    assert not rs.stopped
                    await trio.sleep(10)
                    assert rs.stopped
            finally:
                await st.run("data : get -rd_", do_stdout=False)
            await trio.sleep(11)

            logger.info("End sleep")

            assert rs.started > 0
            assert rs.stopped > 0
            assert rs.backoff == 0
            assert rs.result == 43
