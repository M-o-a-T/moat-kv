import pytest
import anyio
import trio
import mock
from time import time

from trio_click.testing import CliRunner
from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError

import logging
logger = logging.getLogger(__name__)

# This is a basic Trio test which we keep around to check that
# (a) the autojump clock works as advertised
# (b) we can use anyio.
@pytest.mark.trio
async def test_00_anyio_clock(autojump_clock):
    assert trio.current_time() == 0
    t = time()

    for i in range(10):
        start_time = trio.current_time()
        await anyio.sleep(i)
        end_time = trio.current_time()

        assert end_time - start_time == i
    assert time()-t < 1

@pytest.mark.trio
async def test_00_runner(autojump_clock):
    with pytest.raises(AssertionError):
        await run('--doesnotexist')
    await run('--doesnotexist', expect_exit=2)
    #await run('pdb','pdb')  # used for verifying that debugging works

@pytest.mark.trio
async def test_01_basic(autojump_clock):
    async with stdtest(args={'init':123}) as st:
        s, = st.s
        c = await st.client()
        assert (await c.request("get_value", path=())).value == 123

        r = await run("client","-p",s.port,"set","-v","hello","foo")
        r = await run("client","-p",s.port,"set","-ev","'baz'","foo","bar")

        r = await run("client","-p",s.port,"get")
        assert r.stdout == "123\n"

        r = await run("client","-p",s.port,"get", "foo")
        assert r.stdout == "'hello'\n"

        r = await run("client","-p",s.port,"get", "foo", "bar")
        assert r.stdout == "'baz'\n"

        r = await c.request("get_state", nodes=True, known=True, missing=True, remote_missing=True)
        del r['tock']
        assert r == {'nodes': {'test_0': 3}, 'known': {'test_0': ((1, 4),)}, 'missing': {}, 'remote_missing': {}, 'seq': 2}

        assert (await c.request("get_value", node="test_0", tick=1)).value == 123
        assert (await c.request("get_value", node="test_0", tick=2)).value == "hello"
        assert (await c.request("get_value", node="test_0", tick=3)).value == "baz"

        r = await c.request("set_value", path=(), value=1234, nchain=3)
        assert r.prev==123
        assert r.chain.tick == 4

        # does not yet exist
        with pytest.raises(ServerError):
            await c.request("get_value", node="test_0", tick=8)

