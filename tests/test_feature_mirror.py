import logging

import pytest
from moat.util import P

from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_61_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=30) as st:
        assert st is not None
        async with st.client() as c:
            async with c.mirror(P("foo"), need_wait=True) as m:
                assert (await c.get(P(":"))).value == 123

                r = await c.set(P("foo"), value="hello", nchain=3)
                r = await c.set(P("foo.bar"), value="baz", nchain=3)
                await m.wait_chain(r.chain)
                assert m.value == "hello"
                assert m["bar"].value == "baz"

                pass  # mirror end
            pass  # client end
        pass  # server end
