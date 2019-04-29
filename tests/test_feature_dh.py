import pytest

from .mock_serf import stdtest

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_51_dh(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            assert len(s._clients) == 1
            sc = list(s._clients)[0]
            assert c._dh_key is None
            assert sc._dh_key is None
            dh = await c.dh_secret(length=10)
            assert dh == c._dh_key
            assert dh == sc._dh_key
