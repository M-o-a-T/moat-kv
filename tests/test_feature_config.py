import pytest
import trio

from .mock_serf import stdtest
from .run import run

# from .run import run
# from functools import partial

from distkv.auth import loader
from distkv.client import ServerError
from distkv.util import PathLongener

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_81_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            assert 'hoo' not in c.config
            res = await c.set(".distkv","config","hoo", value={"hello":"there"}, nchain=2)
            await c._config.wait_chain(res.chain)
            assert c.config.hoo['hello'] == "there"

        # TODO test iterating on changes
