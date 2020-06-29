import pytest
import jsonschema
from functools import partial

from distkv.mock import run
from distkv.mock.mqtt import stdtest

from distkv.client import ServerError
from distkv.exceptions import ClientAuthRequiredError, ClientAuthMethodError
from distkv.auth import gen_auth
from distkv.util import P

import logging

logger = logging.getLogger(__name__)


@pytest.mark.trio
async def test_22_auth_basic(autojump_clock):  # pylint: disable=unused-argument
    async with stdtest(args={"init": 123}, tocks=50) as st:
        (s,) = st.s
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "client", "-h", h, "-p", p)

        async with st.client() as c:
            assert (await c.get(P(":"))).value == 123

        r = await run_c("data", "get", ":")
        assert r.stdout == "123\n"

        r = await run_c("auth", "-m", "root", "user", "add")

        r = await run_c("data", "get", ":")
        assert r.stdout == "123\n"

        r = await run_c("auth", "-m", "root", "init")

        with pytest.raises(ClientAuthRequiredError):
            await run_c("data", "get", ":")
        with pytest.raises(ClientAuthRequiredError):
            async with st.client() as c:
                assert (await c.get(P(":"))).value == 123

        r = await run_c("-a", "root", "data", "get", ":")
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
    async with stdtest(args={"init": 123}, tocks=80) as st:
        (s,) = st.s
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "client", "-h", h, "-p", p)
        await run_c("data", "set", "-v", "there", "hello")

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

        with pytest.raises(ClientAuthMethodError):
            await run_c("-a", "root", "data", "get", "hello")
        with pytest.raises(ClientAuthRequiredError):
            await run_c("data", "get", "hello")
        with pytest.raises(jsonschema.ValidationError):
            await run_c("-a", "_test", "data", "get", "hello")

        run_t = partial(run_c, "-a", "_test name=fubar")
        res = await run_t("data", "get", "hello")
        assert res.stdout == "'there'\n"


@pytest.mark.trio
async def test_24_auth_password(autojump_clock):
    async with stdtest(args={"init": 123}, tocks=99) as st:
        (s,) = st.s
        autojump_clock.autojump_threshold = 1
        h = p = None
        for h, p, *_ in s.ports:
            if h[0] != ":":
                break
        run_c = partial(run, "-D", "client", "-h", h, "-p", p)
        await run_c("data", "set", "-v", "42", "answers.life etc:.")

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
        await run_c("-a", "root", "data", "set", "-v", 42, "answers.life etc:.")
        with pytest.raises(ClientAuthMethodError):
            res = await run_u("data", "get", "answers.life etc:.")
        await run_c("-a", "root", "auth", "-m", "password", "init", "-s")
        res = await run_u("data", "get", "answers.life etc:.")
        assert res.stdout == "42\n"
        run_u = partial(run_c, "-a", "password name=joe password=test1234")
        with pytest.raises(ServerError) as se:
            res = await run_u("data", "get", "answers.life etc:.")
        assert str(se.value).startswith("AuthFailedError(")
        assert "hashes do not match" in str(se.value)
