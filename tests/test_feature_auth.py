import logging
from functools import partial

import jsonschema
import pytest
from moat.src.test import raises
from moat.util import P

from moat.kv.auth import gen_auth
from moat.kv.client import ServerError
from moat.kv.exceptions import ClientAuthMethodError, ClientAuthRequiredError
from moat.kv.mock import run
from moat.kv.mock.mqtt import stdtest

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_22_auth_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        assert st is not None
        (s,) = st.s
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "kv", "-h", h, "-p", p)

        async with st.client() as c:
            assert (await c.get(P(":"))).value == 123

        r = await run_c("data", ":", "get")
        assert r.stdout == "123\n"

        r = await run_c("auth", "-m", "root", "user", "add")

        r = await run_c("data", ":", "get")
        assert r.stdout == "123\n"

        r = await run_c("auth", "-m", "root", "init")

        with raises(ClientAuthRequiredError):
            await run_c("data", ":", "get")
        with raises(ClientAuthRequiredError):
            async with st.client() as c:
                assert (await c.get(P(":"))).value == 123

        r = await run_c("-a", "root", "data", ":", "get")
        assert r.stdout == "123\n"

        anull = gen_auth("root")
        async with st.client(auth=anull) as c:
            assert (await c.get(())).value == 123

        r = await run_c("-a", "root", "auth", "user", "list")
        assert r.stdout == "*\n"

        r = await run_c("-a", "root", "auth", "user", "list", "-v")
        assert (
            r.stdout
            == """\
ident: '*'
kind: user
typ: root
"""
        )


@pytest.mark.trio
async def test_23_auth_test(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=120) as st:
        assert st is not None
        (s,) = st.s
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "kv", "-h", h, "-p", p)
        await run_c("data", "hello", "set", "-v", ":", "there")

        await run_c("auth", "-m", "root", "user", "add")
        await run_c("auth", "-m", "root", "init")

        run_a = partial(run_c, "-a", "root", "auth", "-m", "_test")
        await run_a("user", "add", "name=fubar")
        res = await run_a("user", "list")
        assert res.stdout == "fubar\n"

        res = await run_a("user", "list", "-v")
        assert (
            res.stdout
            == """\
ident: fubar
kind: user
typ: _test
"""
        )
        await run_c("-a", "root", "auth", "-m", "_test", "init", "-s")

        with raises(ClientAuthMethodError):
            await run_c("-a", "root", "data", "hello")
        with raises(ClientAuthRequiredError):
            await run_c("data", "hello")
        with raises(jsonschema.ValidationError):
            await run_c("-a", "_test", "data", "hello")

        run_t = partial(run_c, "-a", "_test name=fubar")
        res = await run_t("data", "hello")
        assert res.stdout == "'there'\n"


@pytest.mark.trio
async def test_24_auth_password(autojump_clock):
    async with stdtest(args={"init": 123}, tocks=99) as st:
        assert st is not None
        (s,) = st.s
        autojump_clock.autojump_threshold = 1
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "kv", "-h", h, "-p", p)
        await run_c("data", "answers.life etc:.", "set", "-e", ":", "42")

        await run_c("auth", "-m", "root", "user", "add")
        await run_c("auth", "-m", "root", "init")

        run_p = partial(run_c, "-a", "root", "auth", "-m", "password")
        await run_p("user", "add", "name=joe", "password=test123")
        res = await run_p("user", "list")
        assert res.stdout == "joe\n"
        res = await run_p("user", "list", "-v")
        assert (
            res.stdout
            == """\
ident: joe
kind: user
typ: password
"""
        )
        run_u = partial(run_c, "-a", "password name=joe password=test123")
        await run_c("-a", "root", "data", "answers.life etc:.", "set", "-e", ":", 42)
        with raises(ClientAuthMethodError):
            res = await run_u("data", "answers.life etc:.")
        await run_c("-a", "root", "auth", "-m", "password", "init", "-s")
        res = await run_u("data", "answers.life etc:.")
        assert res.stdout == "42\n"
        run_u = partial(run_c, "-a", "password name=joe password=test1234")
        with raises(ServerError) as se:
            res = await run_u("data", "answers.life etc:.")
        assert str(se.value).startswith("AuthFailedError(")
        assert "hashes do not match" in str(se.value)
