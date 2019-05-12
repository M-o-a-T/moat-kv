import pytest
import trio
import time

from .mock_serf import stdtest

from .run import run
from functools import partial

from distkv.client import ServerError
from distkv.util import PathLongener

from distkv.code import CodeRoot, ModuleRoot
from distkv.runner import RunnerRoot
from distkv.errors import ErrorRoot
import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_81_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            await cr.add("forty", "two", code="return 42")
            assert cr("forty.two") == 42


@pytest.mark.trio
async def test_82_module(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            m = await ModuleRoot.as_handler(c)
            await m.add(
                "bar",
                "baz",
                code="""
def quux():
    return 42
""",
            )
            cr = await CodeRoot.as_handler(c)
            await cr.add(
                "forty",
                "two",
                code="""
from bar.baz import quux
return quux()
""",
            )
            assert cr("forty.two") == 42
