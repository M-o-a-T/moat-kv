import logging

import pytest
from moat.src.test import raises
from moat.util import P, PathLongener

from moat.kv.auth import loader
from moat.kv.client import ServerError
from moat.kv.mock.mqtt import stdtest

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
async def test_81_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        assert st is not None
        async with st.client() as c:
            # TODO control what happens when stepping to where's no ACL
            # await c._request("set_internal", path=("acl", "foo"), value="x")
            await c._request("set_internal", path=("acl", "foo", "one"), value="rxnc")
            await c._request("set_internal", path=P("acl.foo.one.two"), value="rc")

            um = loader("_test", "user", make=True, server=False)
            u = um.build({"name": "std"})
            await u.send(c)
            u = um.build({"name": "aclix"})
            await u.send(c)
            await c._request(
                "set_internal",
                path=P("auth._test.user.aclix.acl"),
                value=dict(key="foo"),
                iter=False,
            )
            await c._request("set_auth_typ", typ="_test")
            # , "aux": {"acl": "foo"}})

        um = loader("_test", "user", make=False, server=False)

        async with st.client(auth=um.build({"name": "aclix"})) as c:
            await c.set(P("one"), value=10)
            await c.set(P("one.two"), value=11)
            with raises(ServerError):
                await c.set(P("one.two.three"), value=12)
            with raises(ServerError):
                await c.set(P("one.two"), value=22)


#           with raises(ServerError):
#               await c.set("foo", value=23)
