import logging

import anyio
import pytest
import trio
from asyncscope import scope
from moat.src.test import raises
from moat.util import P, PathLongener

from moat.kv.client import ServerError
from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_21_load_save(autojump_clock, tmpdir):  # pylint: disable=unused-argument
    """also used to check watching"""
    path = tmpdir.join("foo")
    logger.debug("START")
    msgs = []
    s = None

    async def watch_changes(c, evt):
        lg = PathLongener(())
        async with c.watch(P(":"), nchain=3, fetch=True) as res:
            evt.set()
            async for m in res:
                logger.info(m)
                lg(m)
                if m.get("value", None) is not None:
                    msgs.append(m)

    async with stdtest(args={"init": 234}, tocks=30) as st:
        assert st is not None
        (s,) = st.s
        async with st.client() as c:
            assert (await c.get(P(":"))).value == 234
            evt = anyio.Event()
            cs = await scope.spawn(watch_changes, c, evt)
            await evt.wait()

            await c.set(P("foo"), value="hello", nchain=3)
            await c.set(P("foo.bar"), value="baz", nchain=3)
            await c.set(P(":"), value=2345, nchain=3)
            await trio.sleep(1)  # allow the writer to write
            cs.cancel()
            pass  # client end

        logger.debug("SAVE %s", path)
        await s.save(path)
        logger.debug("SAVED")
        pass  # server end

    for m in msgs:
        m.pop("tock", None)
        m.pop("seq", None)
    assert sorted(msgs, key=lambda x: x.chain.tick if "chain" in x else 0) == [
        {
            "chain": {"node": "test_0", "prev": None, "tick": 1},
            "path": P(":"),
            "value": 234,
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 2},
            "path": P("foo"),
            "value": "hello",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 3},
            "path": P("foo.bar"),
            "value": "baz",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 4},
            "path": P(":"),
            "value": 2345,
        },
    ]

    msgs = []
    async with stdtest(run=False, tocks=40) as st:
        assert st is not None
        (s,) = st.s
        logger.debug("LOAD %s", path)
        await s.load(path, local=True)
        logger.debug("LOADED")

        evt = anyio.Event()
        await st.run_0(ready_evt=evt)
        await evt.wait()

        logger.debug("RUNNING")

        async with st.client() as c:
            evt = anyio.Event()
            cs = await scope.spawn(watch_changes, c, evt)
            await evt.wait()

            await c.set(P("foof"), value="again")
            assert (await c.get(P("foo"))).value == "hello"
            assert (await c.get(P("foo.bar"))).value == "baz"
            assert (await c.get(P(":"))).value == 2345
            await trio.sleep(1)  # allow the writer to write
            cs.cancel()

    for m in msgs:
        m.pop("tock", None)
        m.pop("seq", None)
    assert sorted(msgs, key=lambda x: x.chain.tick if "chain" in x else 0) == [
        {
            "chain": {"node": "test_0", "prev": None, "tick": 2},
            "path": P("foo"),
            "value": "hello",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 3},
            "path": P("foo.bar"),
            "value": "baz",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 4},
            "path": P(":"),
            "value": 2345,
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 5},
            "path": P("foof"),
            "value": "again",
        },
    ]
    logger.debug("OK")


@pytest.mark.trio
async def test_02_cmd(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        assert st is not None
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
            assert (
                await c._request("get_value", node="test_0", tick=2)
            ).value == "hello"
            assert (await c._request("get_value", node="test_0", tick=3)).value == "baz"

            r = await c.set(P(":"), value=1234, nchain=3)
            assert r.prev == 123
            assert r.chain.tick == 4

            # does not yet exist
            with raises(ServerError):
                await c._request("get_value", node="test_0", tick=8)
            # has been superseded
            with raises(ServerError):
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
        assert st is not None
        async with st.client(1) as ci:
            assert (await ci.get(P(":"))).value == 125

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
                    present=True,
                    missing=True,
                    remote_missing=True,
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

                assert (await c.get(P(":"))).value == 125

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
                await trio.sleep(1)

                with raises(ServerError):
                    await c._request("get_value", node="test_1", tick=1)
                with raises(ServerError):
                    await ci._request("get_value", node="test_1", tick=1)

                # Now test that the internal states match.
                await trio.sleep(1)
                r = await c._request(
                    "get_state",
                    nodes=True,
                    known=True,
                    present=True,
                    missing=True,
                    remote_missing=True,
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
                pass  # client2 end

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
