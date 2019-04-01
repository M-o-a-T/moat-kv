import pytest
import trio
import mock
from time import time
from functools import partial

from trio_click.testing import CliRunner
from .mock_serf import stdtest
from .run import run
from distkv.client import ServerError
from distkv.util import PathLongener
from distkv.exceptions import ClientAuthRequiredError
from distkv.auth import gen_auth

import logging
logger = logging.getLogger(__name__)

@pytest.mark.trio
async def test_02_cmd(autojump_clock):
    async with stdtest(args={'init':123}) as st:
        s, = st.s
        run_c = partial(run,"client","-h",s.ports[0][0],"-p",s.ports[0][1])

        async with st.client() as c:
            assert (await c.request("get_value", path=())).value == 123

        r = await run_c("get")
        assert r.stdout == "123\n"

        r = await run_c("auth","-m","_null","user","add")

        r = await run_c("get")
        assert r.stdout == "123\n"

        r = await run_c("auth","-m","_null","init")

        with pytest.raises(ClientAuthRequiredError):
            await run_c("get")
        with pytest.raises(ClientAuthRequiredError):
            async with st.client() as c:
                assert (await c.request("get_value", path=())).value == 123

        r = await run_c("-a","_null","get")
        assert r.stdout == "123\n"

        anull = gen_auth("_null")
        async with st.client(auth=anull) as c:
            assert (await c.request("get_value", path=())).value == 123
