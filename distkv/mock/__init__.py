# from asyncclick.testing import CliRunner
import io
import logging
import socket
import sys

import attr
from moat.util import attrdict, combine_dict, list_ext, load_ext, wrap_main

from distkv.client import open_client
from distkv.default import CFG

logger = logging.getLogger(__name__)

CFG = attrdict(**CFG)  # shallow copy
for n, _ in list_ext("distkv_ext"):
    try:
        CFG[n] = combine_dict(
            load_ext("distkv_ext", n, "config", "CFG"), CFG.get(n, {}), cls=attrdict
        )
    except ModuleNotFoundError:
        pass

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager


async def run(*args, expect_exit=0, do_stdout=True):
    args = ("-c", "/dev/null", *args)
    if do_stdout:
        CFG["_stdout"] = out = io.StringIO()
    try:
        res = await wrap_main(
            args=args,
            wrap=True,
            CFG=CFG,
            cfg=False,
            name="distkv",
            sub_pre="distkv.command",
            sub_post="cli",
        )
        if res is None:
            res = attrdict()
        return res
    except SystemExit as exc:
        res = exc
        assert exc.code == expect_exit, exc.code
        return exc
    except BaseException as exc:
        res = exc
        raise
    else:
        assert expect_exit == 0
        return res
    finally:
        if do_stdout:
            res.stdout = out.getvalue()
            CFG["_stdout"] = sys.stdout


@attr.s
class S:
    tg = attr.ib()
    client_ctx = attr.ib()
    s = attr.ib(factory=list)  # servers
    c = attr.ib(factory=list)  # clients
    _seq = 1

    async def ready(self, i=None):
        if i is not None:
            await self.s[i].is_ready
            return self.s[i]
        for s in self.s:
            if s is not None:
                await s.is_ready
        return self.s

    def __iter__(self):
        return iter(self.s)

    @asynccontextmanager
    async def client(self, i: int = 0, **kv):
        """Get a client for the i'th server."""
        await self.s[i].is_serving
        self._seq += 1
        for host, port, *_ in self.s[i].ports:
            if host != "::" and host[0] == ":":
                continue
            try:
                cfg = combine_dict(
                    dict(connect=dict(host=host, port=port, ssl=self.client_ctx, **kv)), CFG
                )
                async with open_client(_main_name="_client_%d_%d" % (i, self._seq), **cfg) as c:
                    yield c
                    return
            except socket.gaierror:
                pass
        raise RuntimeError("Duh? no connection")

    async def run(self, *args, do_stdout=True):
        h = p = None
        for s in self.s:
            for h, p, *_ in s.ports:
                if h[0] != ":":
                    break
            else:
                continue
            break
        if len(args) == 1:
            args = args[0]
            if isinstance(args, str):
                args = args.split(" ")
        return await run("-v", "-v", "client", "-h", h, "-p", p, *args, do_stdout=do_stdout)
