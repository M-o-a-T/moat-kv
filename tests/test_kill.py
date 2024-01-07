import logging

import pytest
import trio
from moat.src.test import raises
from moat.util import P

from moat.kv.client import ServerError
from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_11_kill(autojump_clock):  # pylint: disable=unused-argument
    """also used to check watching"""
    async with stdtest(args={"init": 234}, n=3, tocks=2000) as st:
        assert st is not None
        async with st.client(1) as c:
            await c.set(P("foo"), value="hello")
            await c.set(P("foo.bar"), value="baz")
        await trio.sleep(1)

        async with st.client(2) as c:
            await c.set(P("foo"), value="super")
            await c.set(P("foo.bar"), value="seded")
        await trio.sleep(1)

        async with st.client(0) as c:
            res = await c.get(P("foo.bar"), nchain=3)
            assert res.chain == dict(
                node="test_2", tick=2, prev=dict(node="test_1", tick=2, prev=None)
            )

            res = await c._request("enum_node", node="test_1", current=True)
            assert res.result == ()

            res = await c._request("enum_node", node="test_1", current=False)
            assert res.result == (1, 2)

            res = await c._request("enum_node", node="test_2", current=True)
            assert res.result == (1, 2)

            # error
            with raises(ServerError):
                res = await c._request("kill_node", node="test_2", iter=False)

            # works
            res = await c._request("kill_node", node="test_1")
            await trio.sleep(2)

            res = await c.get(P("foo.bar"), nchain=3)
            assert res.chain == dict(node="test_2", tick=2, prev=None)
