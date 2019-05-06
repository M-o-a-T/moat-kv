import pytest
import trio

from .mock_serf import stdtest

# from .run import run
# from functools import partial

from distkv.auth import loader
from distkv.client import ServerError
from distkv.util import PathLongener

from distkv.code import get_code_handler
from distkv.errors import get_error_handler
import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_81_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            await get_error_handler(c)
            c = await get_code_handler(c)
            await c.add("forty","two", code="return 42")
            assert c("forty.two") == 42

