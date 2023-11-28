import io
import logging
from functools import partial

import pytest
from moat.src.test import raises
from moat.util import P, PathLongener

from moat.kv.client import ServerError
from moat.kv.mock import run
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
async def test_71_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=80) as st:
        assert st is not None
        async with st.client() as c:
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int"),
                    value={
                        "bad": ["foo", None],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): rise ValueError('not an int')",
                        # yes this checks for the typo (SyntaxError=
                    },
                )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int"),
                    value={
                        "bad": ["foo", None],
                        "good": [0, 1, "dud"],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int"),
                    value={
                        "bad": ["foo", 4],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int"),
                    value={
                        "bad": [],
                        "good": [0, 1, 2],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int"),
                    value={
                        "bad": ["foo", None],
                        "good": [1],
                        "code": "if not isinstance(value,int): raise ValueError('not an int')",
                    },
                )
            await c._request(
                "set_internal",
                path=P("type.int"),
                value={
                    "bad": ["foo", None],
                    "good": [0, 1, 2],
                    "code": "if not isinstance(value,int): raise ValueError('not an int')",
                },
            )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int.percent"),
                    value={
                        "bad": ["fuf", 101],
                        "good": [0, 55, 100],
                        "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                    },
                )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("type.int.percent"),
                    value={
                        "bad": ["fuf", 101],
                        "good": [0, 5.5, 100],
                        "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                    },
                )
            await c._request(
                "set_internal",
                path=P("type.int.percent"),
                value={
                    "bad": [-1, 101],
                    "good": [0, 55, 100],
                    "code": "if not 0<=value<=100: raise ValueError('not a percentage')",
                },
            )
            with raises(ServerError):
                await c._request(
                    "set_internal",
                    path=P("match.one.+.two"),
                    value={"tope": P("int.percent")},
                )
            await c._request(
                "set_internal",
                path=P("match.one.+.two"),
                value={"type": P("int.percent")},
            )

            await c.set(P("one.x.two"), value=99)
            with raises(ServerError):
                await c.set(P("one.y.two"), value=9.9)
            with raises(ServerError):
                await c.set(P("one.y.two"), value="zoz")
            await c.set(P("one.y"), value="zoz")

            pass  # client end
        pass  # server end


@pytest.mark.trio
async def test_72_cmd(autojump_clock, tmpdir):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=80) as st:
        assert st is not None
        (s,) = st.s
        async with st.client() as c:
            h = p = None  # pylint
            for h, p, *_ in s.ports:
                if h[0] != ":":
                    break
            rr = partial(run, "kv", "-h", h, "-p", p, do_stdout=False)
            path = tmpdir.join("foo")
            with io.open(path, "w", encoding="utf-8") as f:
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
            await rr("type", "set", "-d", str(path), "int")

            with io.open(path, "w", encoding="utf-8") as f:
                f.write("if not 0<=value<=100: raise ValueError('not a percentage')\n")

            with raises(ServerError):
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
                    "int.percent",
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
                "int.percent",
            )

            await rr("type", "match", "-t", "int.percent", "foo.+.bar")

            with raises(ServerError):
                await rr("data", "foo.dud.bar", "set", "-v", ":", "123")
            with raises(ServerError):
                await rr("data", "foo.dud.bar", "set", "-e", ":", "123")
            with raises(ServerError):
                await rr("data", "foo.dud.bar", "set", "-e", ":", "5.5")
            await rr("data", "foo.dud.bar", "set", "-e", ":", "55")

            assert (await c.get(P("foo.dud.bar"))).value == 55

            pass  # client end
        pass  # server end
