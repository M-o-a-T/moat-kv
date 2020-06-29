import pytest

from distkv.mock.mqtt import stdtest

from distkv.code import CodeRoot, ModuleRoot
from distkv.errors import ErrorRoot
from distkv.util import P
import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_81_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}) as st:
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            cr = await CodeRoot.as_handler(c)
            await cr.add(P("forty.two"), code="return 42")
            assert cr("forty.two") == 42


@pytest.mark.trio
async def test_82_module(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=40) as st:
        async with st.client() as c:
            await ErrorRoot.as_handler(c)
            m = await ModuleRoot.as_handler(c)
            await m.add(
                P("bar.baz"),
                code="""
def quux():
    return 42
""",
            )
            cr = await CodeRoot.as_handler(c)
            await cr.add(
                P("forty.two"),
                code="""
from bar.baz import quux
return quux()
""",
            )
            assert cr("forty.two") == 42
