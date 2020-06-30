import pytest
import trio
import mock

# doesn't work with MQTT because we can't split
from distkv.mock.serf import stdtest
import asyncserf
import msgpack
from distkv.util import attrdict, P, Path

import logging

logger = logging.getLogger(__name__)

N = 10


def _skip_check(self):
    if getattr(self._client, "prefix", "") != "test.core":
        return False
    if not hasattr(self, "_test_foo") or not self._test_foo:
        self._test_foo = ["test_2", "test_3", "test_4"]
    self.logger.debug("SKIP? %s %r %r", self._name, self._test_foo, self._history)
    p = self._test_foo[-1]
    if p in self._history:
        self._test_foo.pop()
        return False
    if self._name == p:
        return True
    return False


@pytest.mark.trio
async def test_10_many(autojump_clock):  # pylint: disable=unused-argument
    """
    This test starts multiple servers at the same time and checks that
    final object deletion works.
    """
    async with stdtest(test_1={"init": 420}, n=N, tocks=1500) as st:
        st.ex.enter_context(mock.patch("asyncactor.Actor._skip_check", new=_skip_check))

        s = st.s[1]
        async with st.client(1) as ci:
            assert (await ci.get(P(":"))).value == 420
            await ci.set(P("ping"), value="pong")
            await ci.set(P("delete.me"), value="later")

            await ci._request(
                "set_internal",
                path=P("actor.del"),
                value={"nodes": "test_2 test_3 test_4".split()},
            )

        await trio.sleep(1)

        async def s1(i, *, task_status=trio.TASK_STATUS_IGNORED):
            async with st.client(i) as c:
                task_status.started()
                assert (await c.get(P(":"))).value == 420
                assert (await c.get(P("ping"))).value == "pong"
                await c.set(Path("foo", i), value=420 + i)

        async with trio.open_nursery() as tg:
            for i in range(1, N):
                await tg.start(s1, i)

        async with st.client(2) as ci:
            assert (await ci.get(P("delete.me"))).value == "later"
            await ci.delete(P("delete.me"))

        await trio.sleep(1)
        NN = min(N - 1, 3)
        for j in [0] + s._actor._rand.sample(range(1, N), NN):
            async with st.client(j) as c:
                for i in s._actor._rand.sample(range(1, N), NN):
                    assert (await c.get(Path("foo", i))).value == 420 + i

        async with st.client(N - 1) as ci:
            r = await ci.get(P("delete.me"), nchain=2)
            assert "value" not in r
            assert r.chain is not None

            with trio.fail_after(9999):
                while True:
                    r = await ci.get(P("delete.me"), nchain=2)
                    if "value" not in r and r.chain is None:
                        break
                    await trio.sleep(10)

        pass  # server end


@pytest.mark.trio
@pytest.mark.parametrize("tocky", [-10, -2, -1, 0, 1, 2, 10])
async def test_11_split1(autojump_clock, tocky):  # pylint: disable=unused-argument
    """
    This test checks that re-joining a split network updates everything.
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
                            r.data, object_pairs_hook=attrdict, raw=False, use_list=False
                        )
                        if msg.get("value", "") == "two":
                            n_two += 1

                    i.s()

        await st.tg.spawn(watch)
        async with st.client(1) as ci:
            assert (await ci.get(P(":"))).value == 420
            r = await ci.set(P("ping"), value="pong")
            pongtock = r.tock
            await ci.set(P("drop.me"), value="here")

        async def s1(i, *, task_status=trio.TASK_STATUS_IGNORED):
            async with st.client(i) as c:
                task_status.started()
                assert (await c.get(P(":"))).value == 420
                await trio.sleep(5)
                r = await c.get(P("ping"))
                assert r.value == "pong"
                assert r.tock == pongtock
                await c.set(Path("foo", i), value=420 + i)
                pass  # client end

        async with trio.open_nursery() as tg:
            for i in range(1, N):
                await tg.start(s1, i)

        await trio.sleep(30)
        st.split(N // 2)
        if tocky:
            async with st.client(2 if tocky < 0 else N - 2) as ci:
                for i in range(abs(tocky)):
                    await ci.set(Path("one", i), value="two")
        await trio.sleep(30)
        async with st.client(1) as ci:
            await ci.delete(P("drop.me"))

        async with st.client(N - 1) as c:
            r = await c.set(P("ping"), value="pongpang")
            pangtock = r.tock

        await trio.sleep(1)
        async with st.client(0) as c:
            # assert that this value is gone
            r = await c.get(P("drop.me"), nchain=3)
            # assert r.chain is None -- not yet
            assert "value" not in r
        await trio.sleep(1)
        async with st.client(N - 1) as c:
            # assert that this value is still here
            r = await c.get(P("drop.me"), nchain=3)
            assert r.chain is not None
            assert r.value == "here"
        await trio.sleep(1)
        async with st.client(0) as c:
            r = await c.get(P("ping"))
            assert r.value == "pong"
            assert r.tock == pongtock

        st.join(N // 2)
        await trio.sleep(200)
        async with st.client(0) as c:
            r = await c.get(P("ping"))
            assert r.value == "pongpang"
            assert r.tock == pangtock

        async with st.client(N - 1) as c:
            # assert that this value is now gone
            r = await c.get(P("drop.me"), nchain=3)
            # assert r.chain is None -- not yet
            assert "value" not in r
        pass  # server end

    # Now make sure that updates are transmitted once
    assert n_two <= 2 * N + 1
