import pytest
import trio
from time import time

import asyncclick as click

from distkv.mock import run
from distkv.mock.serf import stdtest

from distkv.client import ServerError
from distkv.util import PathLongener, P

import logging

logger = logging.getLogger(__name__)


# This is a basic Trio test which we keep around to check that
# (a) the autojump clock works as advertised
# (b) we can use trio.
@pytest.mark.trio
async def test_00_trio_clock(autojump_clock):  # pylint: disable=unused-argument
    assert trio.current_time() == 0
    t = time()

    for i in range(10):
        start_time = trio.current_time()
        await trio.sleep(i)
        end_time = trio.current_time()

        assert end_time - start_time == i
    assert time() - t < 1


@pytest.mark.trio
async def test_00_runner(autojump_clock):  # pylint: disable=unused-argument
    with pytest.raises(click.exceptions.NoSuchOption):
        await run("--doesnotexist")
    # await run("--doesnotexist", expect_exit=2)
    # await run('pdb','pdb')  # used for verifying that debugging works


async def collect(i, path=()):
    res = []
    pl = PathLongener(path)
    async for r in i:
        r.pop("tock", 0)
        r.pop("seq", 0)
        r.pop("wseq", 0)
        pl(r)
        res.append(r)
    return res


@pytest.mark.trio
async def test_01_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        async with st.client() as c:
            assert (await c.get(P(":"))).value == 123

            r = await c.set(P("foo"), value="hello", nchain=3)
            r = await c.set(P("foo.bar"), value="baz", nchain=3)
            bart = r.tock
            r = await c.set(P("foo.baz"), "quux", nchain=3)
            r = await c.get(P(":"))
            assert r.value == 123
            assert r.tock < await c.get_tock()

            r = await c.get(P("foo"))
            assert r.value == "hello"

            exp = [
                {"path": (), "value": 123},
                {"path": P("foo"), "value": "hello"},
                {"path": P("foo.bar"), "value": "baz"},
                {"path": P("foo.baz"), "value": "quux"},
            ]
            r = await c.list(P(":"))
            assert r == (None, ".distkv", "foo")
            r = await c.list(P("foo"))
            assert r == ("bar", "baz")
            r = await c.list(P("foo"), with_data=True)
            assert r == dict(bar="baz", baz="quux")
            r = await c.list(P("foo.bar"))
            assert r == ()

            async with c._stream("get_tree", path=(), max_depth=2) as rr:
                r = await collect(rr)
            assert r == exp

            async with c._stream("get_tree", path=(), min_depth=1) as rr:
                r = await collect(rr)
            assert r == exp[1:]

            exp.pop()
            exp.pop()
            async with c._stream("get_tree", path=(), iter=True, max_depth=1) as rr:
                r = await collect(rr)
            assert r == exp

            exp.pop()
            async with c._stream("get_tree", path=(), iter=True, max_depth=0) as rr:
                r = await collect(rr)
            assert r == exp

            r = await c.get(P("foo.bar"))
            assert r.value == "baz"
            assert r.tock == bart

            r = await c._request(
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "node": "test_0",
                "nodes": {"test_0": 4},
                "known": {},
                "present": {"test_0": ((1, 5),)},
                "missing": {},
                "remote_missing": {},
            }

            assert (await c._request("get_value", node="test_0", tick=1)).value == 123
            assert (await c._request("get_value", node="test_0", tick=2)).value == "hello"
            assert (await c._request("get_value", node="test_0", tick=3)).value == "baz"

            r = await c.set(P(":"), value=1234, nchain=3)
            assert r.prev == 123
            assert r.chain.tick == 5

            # does not yet exist
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=8)
            # has been superseded
            with pytest.raises(ServerError):
                await c._request("get_value", node="test_0", tick=1)
            # works
            assert (await c._request("get_value", node="test_0", tick=5)).value == 1234

            r = await c.set(P("foo.bar"), value="bazz")
            assert r.tock > bart
            bart = r.tock

            r = await c._request(
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "node": "test_0",
                "nodes": {"test_0": 6},
                "known": {"test_0": (1, 3)},
                "present": {"test_0": (2, (4, 7))},
                "missing": {},
                "remote_missing": {},
            }

            r = await c.delete(P("foo"))
            assert r.tock > bart

            r = await c.get(P("foo.bar"))
            assert r.value == "bazz"
            assert r.tock == bart

            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_02_cmd(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        async with st.client() as c:
            assert (await c.get(P(":"))).value == 123

            r = await st.run("data foo set -v : hello")
            r = await st.run("data foo.bar set -e : 'baz'")

            r = await st.run("data :")
            assert r.stdout == "123\n"

            r = await st.run("data foo")
            assert r.stdout == "'hello'\n"

            r = await st.run("data foo.bar")
            assert r.stdout == "'baz'\n"

            r = await c._request(
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "node": "test_0",
                "nodes": {"test_0": 3},
                "known": {},
                "present": {"test_0": ((1, 4),)},
                "missing": {},
                "remote_missing": {},
            }

            assert (await c._request("get_value", node="test_0", tick=1)).value == 123
            assert (await c._request("get_value", node="test_0", tick=2)).value == "hello"
            assert (await c._request("get_value", node="test_0", tick=3)).value == "baz"

            r = await c.set(P(":"), value=1234, nchain=3)
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
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "node": "test_0",
                "nodes": {"test_0": 4},
                "known": {"test_0": (1,)},
                "present": {"test_0": ((2, 5),)},
                "missing": {},
                "remote_missing": {},
            }
            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_03_three(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(test_1={"init": 125}, n=2, tocks=30) as st:
        async with st.client(1) as ci:
            assert (await ci._request("get_value", path=())).value == 125

            r = await ci._request(
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            # Various stages of integrating test_0
            assert (
                r
                == {
                    "node": "test_1",
                    "nodes": {"test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "node": "test_1",
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "node": "test_1",
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
                    "missing": {"test_0": (1,)},
                    "remote_missing": {"test_0": (1,)},
                }
                or r
                == {
                    "node": "test_1",
                    "nodes": {"test_1": 1, "test_0": None},
                    "known": {},
                    "present": {"test_0": (1,), "test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }
                or r
                == {
                    "node": "test_1",
                    "nodes": {"test_0": 0, "test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
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
                    present=True,
                )
                del r["tock"]
                del r["seq"]
                assert r == {
                    "node": "test_1",
                    "nodes": {"test_0": 0, "test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                } or r == {
                    "node": "test_1",
                    "nodes": {"test_0": None, "test_1": 1},
                    "known": {},
                    "present": {"test_1": (1,)},
                    "missing": {},
                    "remote_missing": {},
                }

                assert (await c._request("get_value", path=())).value == 125

                r = await c.set(P(":"), value=126, nchain=3)
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
                r = await ci.set(P(":"), value=127, nchain=3)
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
                    present=True,
                )
                del r["tock"]
                del r["seq"]
                assert r == {
                    "node": "test_0",
                    "nodes": {"test_0": 1, "test_1": 2},
                    "known": {"test_1": (1,)},
                    "present": {"test_0": (1,), "test_1": (2,)},
                    "missing": {},
                    "remote_missing": {},
                }

            r = await ci._request(
                "get_state",
                nodes=True,
                known=True,
                missing=True,
                remote_missing=True,
                present=True,
            )
            del r["tock"]
            del r["seq"]
            assert r == {
                "node": "test_1",
                "nodes": {"test_0": 1, "test_1": 2},
                "known": {"test_1": (1,)},
                "present": {"test_0": (1,), "test_1": (2,)},
                "missing": {},
                "remote_missing": {},
            }
            pass  # client end
        pass  # server end
