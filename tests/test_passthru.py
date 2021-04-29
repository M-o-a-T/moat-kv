import pytest
import trio

from distkv.mock.serf import stdtest
from distkv.exceptions import CancelledError
from distkv.util import P


@pytest.mark.trio
async def test_51_passthru(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}) as st:
        (s,) = st.s
        async with st.client() as c:
            recv = []

            async def mon():
                try:
                    async with c._stream("msg_monitor", topic=P("foo")) as q:
                        async for m in q:
                            assert "data" in m
                            assert m.topic[0] == "foo"
                            assert m.topic[1] in {"bar", "baz"}
                            recv.append(m.data)
                except CancelledError:
                    pass

            s.spawn(mon)
            await trio.sleep(0.2)
            await c._request("msg_send", topic=P("foo.bar"), data=["Hello", 42])
            await c._request("msg_send", topic=("foo", "baz"), data=b"duh")
            await trio.sleep(0.5)
        assert recv == [("Hello", 42), b"duh"]
        pass  # closing client
    pass  # closing server


@pytest.mark.trio
async def test_52_passthru_bin(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}) as st:
        (s,) = st.s
        async with st.client() as c:
            recv = []

            async def mon():
                try:
                    async with c._stream("msg_monitor", topic=("foo",), raw=True) as q:
                        async for m in q:
                            assert "data" not in m
                            recv.append(m.raw)
                except CancelledError:
                    pass

            s.spawn(mon)
            await trio.sleep(0.2)
            await c._request("msg_send", topic=("foo",), data=["Hello", 42])
            await c._request("msg_send", topic=P("foo"), raw=b"duh")
            await trio.sleep(0.5)
        assert recv == [b"\x92\xa5Hello*", b"duh"]
        pass  # closing client
    pass  # closing server
