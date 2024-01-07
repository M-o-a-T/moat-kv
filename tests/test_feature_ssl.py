import logging

import pytest
from moat.src.test import raises
from moat.util import P, PathLongener

from moat.kv.client import ServerError
from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.xfail()


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
async def test_41_ssl_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(ssl=True, args={"init": 123}) as st:
        assert st is not None
        async with st.client() as c:
            assert (await c.get()).value == 123

            r = await c.set(P("foo"), value="hello", nchain=3)
            r = await c.set(P("foo.bar"), value="baz", nchain=3)
            r = await c.get()
            assert r.value == 123

            r = await c.get("foo")
            assert r.value == "hello"

            exp = [
                {"path": P(":"), "value": 123},
                {"path": P("foo"), "value": "hello"},
                {"path": P("foo.bar"), "value": "baz"},
            ]
            async with c._stream("get_tree", path=P(":"), max_depth=2) as rr:
                r = await collect(rr)
            assert r == exp

            exp.pop()
            async with c._stream("get_tree", path=P(":"), iter=True, max_depth=1) as rr:
                r = await collect(rr)
            assert r == exp

            exp.pop()
            async with c._stream("get_tree", path=P(":"), iter=True, max_depth=0) as rr:
                r = await collect(rr)
            assert r == exp

            r = await c.get(P("foo.bar"))
            assert r.value == "baz"

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

            r = await c.set(value=1234, nchain=3)
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
