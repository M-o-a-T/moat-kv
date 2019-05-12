import pytest
import io
from functools import partial

from .mock_serf import stdtest
from .run import run
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
async def test_71_basic(autojump_clock):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int"),
                    value={
                        "bad": ["foo", None],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): rise ValueError('not an int')",
                    },
                )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int"),
                    value={
                        "bad": ["foo", None],
                        "good": [0, 1, "dud"],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int"),
                    value={
                        "bad": ["foo", 4],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): rise ValueError('not an int')",
                    },
                )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int"),
                    value={
                        "bad": [],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int"),
                    value={
                        "bad": ["foo", None],
                        "good": [1],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            await c._request(
                "set_internal",
                path=("type", "int"),
                value={
                    "bad": ["foo", None],
                    "good": [0, 1, 2],
                    "code": "if not isinstance(value,int): raise ValueError('not an int')",
                },
            )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int", "percent"),
                    value={
                        "bad": ["fuf", 101],
                        "good": [0, 55, 100],
                        "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                    },
                )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("type", "int", "percent"),
                    value={
                        "bad": ["fuf", 101],
                        "good": [0, 5.5, 100],
                        "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                    },
                )
            await c._request(
                "set_internal",
                path=("type", "int", "percent"),
                value={
                    "bad": [-1, 101],
                    "good": [0, 55, 100],
                    "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                },
            )
            with pytest.raises(ServerError):
                await c._request(
                    "set_internal",
                    path=("match", "one", "+", "two"),
                    value={"tope": ("int", "percent")},
                )
            await c._request(
                "set_internal",
                path=("match", "one", "+", "two"),
                value={"type": ("int", "percent")},
            )

            await c.set("one", "x", "two", value=99)
            with pytest.raises(ServerError):
                await c.set("one", "y", "two", value=9.9)
            with pytest.raises(ServerError):
                await c.set("one", "y", "two", value="zoz")
            await c.set("one", "y", value="zoz")

            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_72_cmd(autojump_clock, tmpdir):
    async with stdtest(args={"init": 123}) as st:
        s, = st.s
        async with st.client() as c:
            for h, p, *_ in s.ports:
                if h[0] != ":":
                    break
            rr = partial(run, "client", "-h", h, "-p", p, do_stdout=False)
            path = tmpdir.join("foo")
            with io.open(path, "w") as f:
                f.write(
                    """\
good:
- 0
- 2
bad:
- none
- "Foo"
code: "if not isinstance(value,int): raise ValueError('not an int')"
"""
                )
            await rr("type", "set", "-y", "-s", str(path), "int")

            with io.open(path, "w") as f:
                f.write("if not 0<=value<=100: raise ValueError('not a percentage')\n")

            with pytest.raises(ServerError):
                await rr(
                    "type",
                    "set",
                    "-s",
                    str(path),
                    "-g",
                    "0",
                    "-g",
                    "100",
                    "-g",
                    "50",
                    "-b",
                    "-1",
                    "-b",
                    "5.5",
                    "int",
                    "percent",
                )
            await rr(
                "type",
                "set",
                "-s",
                str(path),
                "-g",
                "0",
                "-g",
                "100",
                "-g",
                "50",
                "-b",
                "-1",
                "-b",
                "555",
                "int",
                "percent",
            )

            await rr("type", "match", "-t", "int", "-t", "percent", "foo", "+", "bar")

            with pytest.raises(ServerError):
                await rr("set", "-v", "123", "foo", "dud", "bar")
            with pytest.raises(ServerError):
                await rr("set", "-ev", "123", "foo", "dud", "bar")
            with pytest.raises(ServerError):
                await rr("set", "-ev", "5.5", "foo", "dud", "bar")
            await rr("set", "-ev", "55", "foo", "dud", "bar")

            assert (await c.get("foo", "dud", "bar")).value == 55

            pass  # client end
        pass  # server end
