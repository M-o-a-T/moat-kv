import pytest
import anyio
import trio
import mock
from time import time

from trio_click.testing import CliRunner
from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError

import logging
logger = logging.getLogger(__name__)

N=20

@pytest.mark.trio
async def test_10_many(autojump_clock):
    """
    This test starts multiple servers at the same time.
    """
    async with stdtest(test_1={'init':420}, n=N, tocks=1000) as st:
        s = st.s[1]
        async with st.client(1) as ci:
            assert (await ci.request("get_value", path=())).value == 420
            await ci.request("set_value", path=("ping",), value="pong")

        await anyio.sleep(1)
        async def s1(i):
            async with st.client(i) as c:
                assert (await c.request("get_value", path=())).value == 420
                assert (await c.request("get_value", path=("ping",))).value == "pong"
                await c.request("set_value",path=("foo",i),value=420+i)

        async with anyio.create_task_group() as tg:
            for i in range(1,N):
                await tg.spawn(s1,i)

        await anyio.sleep(1)
        NN=min(N-1,3)
        for j in [0]+s._random.sample(range(1,N),NN):
            async with st.client(j) as c:
                for i in s._random.sample(range(1,N),NN):
                    assert (await c.request("get_value", path=("foo",i))).value == 420+i

        #await anyio.sleep(100)

@pytest.mark.trio
@pytest.mark.parametrize("tocky", [-10,-2,-1,0,1,2,10])
async def test_11_split1(autojump_clock, tocky):
    """
    This test starts multiple servers at the same time.
    """
    async with stdtest(test_1={'init':420}, n=N, tocks=1000) as st:
        s = st.s[1]
        async with st.client(1) as ci:
            assert (await ci.request("get_value", path=())).value == 420
            await ci.request("set_value", path=("ping",), value="pong")

        async def s1(i):
            async with st.client(i) as c:
                assert (await c.request("get_value", path=())).value == 420
                await anyio.sleep(5)
                assert (await c.request("get_value", path=("ping",))).value == "pong"
                await c.request("set_value",path=("foo",i),value=420+i)

        async with anyio.create_task_group() as tg:
            for i in range(1,N):
                await tg.spawn(s1,i)

        await anyio.sleep(100)
        st.split(N//2)
        if tocky:
            async with st.client(2 if tocky < 0 else 14) as ci:
                for i in range(abs(tocky)):
                    await ci.request("set_value", path=("one",i), value="two")
        await anyio.sleep(100)

        async with st.client(N-1) as c:
            await c.request("set_value", path=("ping",), value="pongpang")
        await anyio.sleep(1)
        async with st.client(0) as c:
            assert (await c.request("get_value", path=('ping',))).value == "pong"
        st.join(N//2)
        await anyio.sleep(20)
        async with st.client(0) as c:
            assert (await c.request("get_value", path=('ping',))).value == "pongpang"


