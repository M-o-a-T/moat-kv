import logging

import pytest
from moat.util import P

from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_81_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}) as st:
        assert st is not None
        async with st.client() as c:
            assert "hoo" not in c.config
            res = await c.set(
                P(":.moat.kv.config.hoo"), value={"hello": "there"}, nchain=2
            )
            await c._config.wait_chain(res.chain)
            assert c.config.hoo["hello"] == "there"

        # TODO test iterating on changes
