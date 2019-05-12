import pytest
import trio
from time import time

from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError
from distkv.util import PathLongener

import logging

logger = logging.getLogger(__name__)


# This is a basic Trio test which we keep around to check that
# (a) the autojump clock works as advertised
# (b) we can use trio.
@pytest.mark.trio
async def test_00_trio_clock(autojump_clock):
    assert trio.current_time() == 0
    t = time()

    for i in range(10):
        start_time = trio.current_time()
        await trio.sleep(i)
        end_time = trio.current_time()

        assert end_time - start_time == i
    assert time() - t < 1


@pytest.mark.trio
async def test_00_runner(autojump_clock):
    with pytest.raises(AssertionError):
        await run("--doesnotexist")
    await run("--doesnotexist", expect_exit=2)
    # await run('pdb','pdb')  # used for verifying that debugging works


async def collect(i, path=()):
    res = []
    pl = PathLongener(path)
    async for r in i:
        r.pop("tock", 0)
        r.pop("seq", 0)
        pl(r)
        res.append(r)
    return res


@pytest.mark.trio
async def test_01_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            assert (await c.get()).value == 123

            r = await c.set("foo", value="hello", nchain=3)
            r = await c.set("foo", "bar", value="baz", nchain=3)
            bart = r.tock
            r = await c.get()
            assert r.value == 123
            assert r.tock < await c.get_tock()

            r = await c.get("foo")
            assert r.value == "hello"

            exp = [
                {"path": (), "value": 123},
                {"path": ("foo",), "value": "hello"},
                {"path": ("foo", "bar"), "value": "baz"},
            ]
            async with c._stream("get_tree", path=(), max_depth=2) as rr:
                r = await collect(rr)
            assert r == exp

            exp.pop()
            async with c._stream("get_tree", path=(), iter=True, max_depth=1) as rr:
                r = await collect(rr)
            assert r == exp

            exp.pop()
            async with c._stream("get_tree", path=(), iter=True, max_depth=0) as rr:
                r = await collect(rr)
            assert r == exp

            r = await c.get("foo", "bar")
            assert r.value == "baz"
            assert r.tock == bart

            r = await c._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "nodes": {"test_0": 3},
                "known": {"test_0": ((1, 4),)},
                "missing": {},
                "remote_missing": {},
            }

            assert (await c._request("get_value", node="test_0", tick=1)).value == 123
            assert (
                await c._request("get_value", node="test_0", tick=2)
            ).value == "hello"
            assert (await c._request("get_value", node="test_0", tick=3)).value == "baz"

            r = await c.set(value=1234, nchain=3)
            assert r.prev == 123
            assert r.chain.tick == 4

            # does not yet exist
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=8)
            # has been superseded
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=1)
            # works
            assert (await c._request("get_value", node="test_0", tick=4)).value == 1234

            r = await c.set("foo", "bar", value="bazz")
            assert r.tock > bart
            bart = r.tock

            r = await c._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "nodes": {"test_0": 5},
                "known": {"test_0": ((1, 6),)},
                "missing": {},
                "remote_missing": {},
            }

            r = await c.delete("foo")
            assert r.tock > bart

            r = await c.get("foo", "bar")
            assert r.value == "bazz"
            assert r.tock == bart

            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_02_cmd(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            assert (await c.get()).value == 123
            for h, p, *_ in s.ports:
                if h[0] != ":":
                    break

            r = await run("client", "-h", h, "-p", p, "set", "-v", "hello", "foo")
            r = await run(
                "client", "-h", h, "-p", p, "set", "-ev", "'baz'", "foo", "bar"
            )

            r = await run("client", "-h", h, "-p", p, "get")
            assert r.stdout == "123\n"

            r = await run("client", "-h", h, "-p", p, "get", "foo")
            assert r.stdout == "'hello'\n"

            r = await run("client", "-h", h, "-p", p, "get", "foo", "bar")
            assert r.stdout == "'baz'\n"

            r = await c._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            assert r == {
                "nodes": {"test_0": 3},
                "known": {"test_0": ((1, 4),)},
                "missing": {},
                "remote_missing": {},
                "seq": 2,
            }

            assert (await c._request("get_value", node="test_0", tick=1)).value == 123
            assert (
                await c._request("get_value", node="test_0", tick=2)
            ).value == "hello"
            assert (await c._request("get_value", node="test_0", tick=3)).value == "baz"

            r = await c._request("set_value", path=(), value=1234, nchain=3)
            assert r.prev == 123
            assert r.chain.tick == 4

            # does not yet exist
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=8)
            # has been superseded
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=1)
            # works
            assert (await c._request("get_value", node="test_0", tick=4)).value == 1234

            r = await c._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "nodes": {"test_0": 4},
                "known": {"test_0": ((1, 5),)},
                "missing": {},
                "remote_missing": {},
            }
            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_03_three(autojump_clock):
    async with stdtest(test_1={"init": 125}, n=2, tocks=30) as st:
        s, si = st.s
        async with st.client(1) as ci:
            assert (await ci._request("get_value", path=())).value == 125

            r = await ci._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            del r["seq"]
            # Various stages of integrating test_0
            assert (
                r
                == {
                    "nodes": {"test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {"test_0": (1,)},
                    "remote_missing": {"test_0": (1,)},
                }
                or r
                == {
                    "nodes": {"test_1": 1, "test_0": None},
                    "known": {"test_0": (1,), "test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "nodes": {"test_0": 0, "test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or False
            )

            # This waits for test_0 to be fully up and running.
            async with st.client(0) as c:

                # At this point ci shall be fully integrated, and test_1 shall know this (mostly).
                r = await ci._request(
                    "get_state",
                    nodes=True,
                    known=True,
                    missing=True,
                    remote_missing=True,
                )
                del r["tock"]
                del r["seq"]
                assert r == {
                    "nodes": {"test_0": 0, "test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                } or r == {
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }

                assert (await c._request("get_value", path=())).value == 125

                r = await c._request("set_value", path=(), value=126, nchain=3)
                assert r.prev == 125
                assert r.chain.tick == 1
                assert r.chain.node == "test_0"
                assert r.chain.prev.tick == 1
                assert r.chain.prev.node == "test_1"
                assert r.chain.prev.tick == 1

                # This verifies that the chain entry for the initial update is gone
                # and the initial change is no longer retrievable.
                # We need the latter to ensure that there are no memory leaks.
                await trio.sleep(1)
                r = await ci._request("set_value", path=(), value=127, nchain=3)
                assert r.prev == 126
                assert r.chain.tick == 2
                assert r.chain.node == "test_1"
                assert r.chain.prev.tick == 1
                assert r.chain.prev.node == "test_0"
                assert r.chain.prev.tick == 1
                assert r.chain.prev.prev is None

                with pytest.raises(ServerError):
                    await c._request("get_value", node="test_1", tick=1)
                with pytest.raises(ServerError):
                    await ci._request("get_value", node="test_1", tick=1)

                # Now test that the internal states match.
                await trio.sleep(1)
                r = await c._request(
                    "get_state",
                    nodes=True,
                    known=True,
                    missing=True,
                    remote_missing=True,
                )
                del r["tock"]
                del r["seq"]
                assert r == {
                    "nodes": {"test_0": 1, "test_1": 2},
                    "known": {"test_0": (1,), "test_1": ((1, 3),)},
                    "missing": {},
                    "remote_missing": {},
                }

            r = await ci._request(
                "get_state", nodes=True, known=True, missing=True, remote_missing=True
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "nodes": {"test_0": 1, "test_1": 2},
                "known": {"test_0": (1,), "test_1": ((1, 3),)},
                "missing": {},
                "remote_missing": {},
            }
            pass  # client end
        pass  # server end
