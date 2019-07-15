import pytest
import trio

from .mock_serf import stdtest

# from .run import run
# from functools import partial

from distkv.auth import loader
from distkv.client import ServerError
from distkv.util import PathLongener

import logging

logger = logging.getLogger(__name__)


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
async def test_81_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            await c._request("set_internal", path=("acl", "foo", "one"), value="rxnc")
            await c._request(
                "set_internal", path=("acl", "foo", "one", "two"), value="rc"
            )

            um = loader("_test", "user", make=True, server=False)
            u = um.build({"name": "std"})
            await u.send(c)
            u = um.build({"name": "aclix", "aux": {"acl": "foo"}})
            await u.send(c)
            await c._request("set_auth_typ", typ="_test")

        recv = []
        um = loader("_test", "user", make=False, server=False)

        async with st.client(auth=um.build({"name": "aclix"})) as c:
            await c.set("one", value=10)
            await c.set("one", "two", value=11)
            with pytest.raises(ServerError):
                await c.set("one", "two", "three", value=12)
            with pytest.raises(ServerError):
                await c.set("foo", value=22)
