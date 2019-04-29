import pytest

from .mock_serf import stdtest

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_61_basic(autojump_clock):
    async with stdtest(args={"init": 123}, tocks=30) as st:
        s, = st.s
        async with st.client() as c:
            async with c.mirror("foo", need_wait=True) as m:
                assert (await c.get()).value == 123

                r = await c.set("foo", value="hello", nchain=3)
                r = await c.set("foo", "bar", value="baz", nchain=3)
                await m.wait_chain(r.chain)
                assert m.value == "hello"
                assert m["bar"].value == "baz"

                pass  # mirror end
            pass  # client end
        pass  # server end
