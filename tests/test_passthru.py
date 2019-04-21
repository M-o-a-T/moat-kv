import pytest
import trio
import mock
from time import time

from trio_click.testing import CliRunner
from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError
from distkv.util import PathLongener

import logging
logger = logging.getLogger(__name__)

@pytest.mark.trio
async def test_51_passthru():
    async with stdtest(args={'init':123}) as st:
        s, = st.s
        async with st.client() as c:
            recv = []
            async def mon():
                async with c.stream("serfmon", type="foo") as q:
                    async for m in p:
                        recv.append(m)
            await s.spawn(mon)
            await trio.sleep(0.2)
            await c.request("serfmon", type="foo", data=["Hello",42])
            await trio.sleep(0.5)
        assert recv == [["hello",42]]

