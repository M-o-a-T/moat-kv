import pytest
import trio
import anyio

from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError
from distkv.util import PathLongener
from functools import partial

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_21_load_save(autojump_clock, tmpdir):
    """also used to check watching"""
    path = tmpdir.join("foo")
    logger.debug("START")
    msgs = []
    s = None

    async def watch_changes(c, evt):
        lg = PathLongener(())
        async with c.watch(nchain=3, fetch=True) as res:
            await evt.set()
            async for m in res:
                logger.info(m)
                lg(m)
                if m.get("value", None) is not None:
                    msgs.append(m)

    async with stdtest(args={"init": 234}, tocks=30) as st:
        s, = st.s
        async with st.client() as c:
            assert (await c.get()).value == 234
            evt = anyio.create_event()
            await c.tg.spawn(watch_changes, c, evt)
            await evt.wait()

            await c.set("foo", value="hello", nchain=3)
            await c.set("foo", "bar", value="baz", nchain=3)
            await c.set(value=2345, nchain=3)
            await trio.sleep(1)  # allow the writer to write
            pass  # client end

        logger.debug("SAVE %s", path)
        await s.save(path)
        logger.debug("SAVED")
        pass  # server end

    logger.debug("NEXT")
    for m in msgs:
        m.pop("tock", None)
        m.pop("seq", None)
    assert sorted(msgs, key=lambda x: x.chain.tick if "chain" in x else 0) == [
        {
            "chain": {"node": "test_0", "prev": None, "tick": 1},
            "path": (),
            "value": 234,
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 2},
            "path": ("foo",),
            "value": "hello",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 3},
            "path": ("foo", "bar"),
            "value": "baz",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 4},
            "path": (),
            "value": 2345,
        },
    ]

    msgs = []
    async with stdtest(run=False, tocks=40) as st:
        s, = st.s
        logger.debug("LOAD %s", path)
        await s.load(path, local=True)
        logger.debug("LOADED")

        evt = anyio.create_event()
        await st.tg.spawn(partial(st.s[0].serve, ready_evt=evt))
        await evt.wait()

        logger.debug("RUNNING")

        async with st.client() as c:
            evt = anyio.create_event()
            await c.tg.spawn(watch_changes, c, evt)
            await evt.wait()

            await c.set("foof", value="again")
            assert (await c.get("foo")).value == "hello"
            assert (await c.get("foo", "bar")).value == "baz"
            assert (await c.get()).value == 2345
            await trio.sleep(1)  # allow the writer to write

    for m in msgs:
        m.pop("tock", None)
        m.pop("seq", None)
    assert sorted(msgs, key=lambda x: x.chain.tick if "chain" in x else 0) == [
        {
            "chain": {"node": "test_0", "prev": None, "tick": 2},
            "path": ("foo",),
            "value": "hello",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 3},
            "path": ("foo", "bar"),
            "value": "baz",
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 4},
            "path": (),
            "value": 2345,
        },
        {
            "chain": {"node": "test_0", "prev": None, "tick": 5},
            "path": ("foof",),
            "value": "again",
        },
    ]
    logger.debug("OK")


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
            assert (await ci.get()).value == 125

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

                assert (await c.get()).value == 125

                r = await c.set(value=126, nchain=3)
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
                r = await ci.set(value=127, nchain=3)
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
                pass  # client2 end

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
