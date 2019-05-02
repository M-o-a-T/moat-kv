import pytest
import trio

from .mock_serf import stdtest
import asyncserf
import msgpack
from distkv.util import attrdict

import logging

logger = logging.getLogger(__name__)

N = 20


@pytest.mark.trio
async def test_10_many(autojump_clock):
    """
    This test starts multiple servers at the same time.
    """
    async with stdtest(test_1={"init": 420}, n=N, tocks=1000) as st:
        s = st.s[1]
        async with st.client(1) as ci:
            assert (await ci.get()).value == 420
            await ci.set("ping", value="pong")

        await trio.sleep(1)

        async def s1(i, *, task_status=trio.TASK_STATUS_IGNORED):
            async with st.client(i) as c:
                task_status.started()
                assert (await c.get()).value == 420
                assert (await c.get("ping")).value == "pong"
                await c.set("foo", i, value=420 + i)

        async with trio.open_nursery() as tg:
            for i in range(1, N):
                await tg.start(s1, i)

        await trio.sleep(1)
        NN = min(N - 1, 3)
        for j in [0] + s._actor._rand.sample(range(1, N), NN):
            async with st.client(j) as c:
                for i in s._actor._rand.sample(range(1, N), NN):
                    assert (await c.get("foo", i)).value == 420 + i

        # await trio.sleep(100)
        pass  # server end


@pytest.mark.trio
@pytest.mark.parametrize("tocky", [-10, -2, -1, 0, 1, 2, 10])
async def test_11_split1(autojump_clock, tocky):
    """
    This test starts multiple servers at the same time.
    """
    n_two = 0

    async with stdtest(test_1={"init": 420}, n=N, tocks=1000) as st:

        async def watch(*, task_status=trio.TASK_STATUS_IGNORED):
            nonlocal n_two
            async with asyncserf.serf_client() as s:
                async with s.stream("user:test.update") as sr:
                    task_status.started()
                    async for r in sr:
                        msg = msgpack.unpackb(
                            r.data,
                            object_pairs_hook=attrdict,
                            raw=False,
                            use_list=False,
                        )
                        if msg.get("value", "") == "two":
                            n_two += 1

                    i.s()

        await st.tg.start(watch)
        async with st.client(1) as ci:
            assert (await ci.get()).value == 420
            r = await ci.set("ping", value="pong")
            pongtock = r.tock

        async def s1(i, *, task_status=trio.TASK_STATUS_IGNORED):
            async with st.client(i) as c:
                task_status.started()
                assert (await c.get()).value == 420
                await trio.sleep(5)
                r = await c.get("ping")
                assert r.value == "pong"
                assert r.tock == pongtock
                await c.set("foo", i, value=420 + i)
                pass  # client end

        async with trio.open_nursery() as tg:
            for i in range(1, N):
                await tg.start(s1, i)

        await trio.sleep(30)
        st.split(N // 2)
        if tocky:
            async with st.client(2 if tocky < 0 else 14) as ci:
                for i in range(abs(tocky)):
                    await ci.set("one", i, value="two")
        await trio.sleep(30)

        async with st.client(N - 1) as c:
            r = await c.set("ping", value="pongpang")
            pangtock = r.tock
        await trio.sleep(1)
        async with st.client(0) as c:
            r = await c.get("ping")
            assert r.value == "pong"
            assert r.tock == pongtock
        st.join(N // 2)
        await trio.sleep(20)
        async with st.client(0) as c:
            r = await c.get("ping")
            assert r.value == "pongpang"
            assert r.tock == pangtock
        pass  # server end

    # Now make sure that updates are transmitted once
    assert n_two <= N + 1
