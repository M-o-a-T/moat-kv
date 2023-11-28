#!/usr/bin/env python3
"""
Basic DistKV support

"""

import logging
from pathlib import Path

import asyncclick as click
from moat.util import attrdict, combine_dict, load_subgroup, yload

from moat.kv.auth import gen_auth
from moat.kv.client import client_scope

logger = logging.getLogger(__name__)


CFG = yload(Path(__file__).parent / "_config.yaml", attr=True)


class NullObj:
    """
    This helper defers raising an exception until one of its attributes is
    actually accessed.
    """

    def __init__(self, exc):
        self._exc = exc

    def __call__(self, *a, **kw):
        raise self._exc

    def __await__(self):
        raise self._exc

    def __getattr__(self, k):
        if k[0] == "_" and k not in ("_request", "_cfg"):
            return object.__getattribute__(self, k)
        raise self._exc


@load_subgroup(
    sub_pre="moat.kv.command", sub_post="cli", ext_pre="moat.kv", ext_post="_main.cli"
)
@click.option(
    "-h", "--host", default=None, help=f"Host to use. Default: {CFG.kv.conn.host}"
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=None,
    help=f"Port to use. Default: {CFG.kv.conn.port}",
)
@click.option(
    "-a",
    "--auth",
    type=str,
    default=None,
    help="Auth params. =file or 'type param=value…' Default: _anon",
)
@click.option("-m", "--metadata", is_flag=True, help="Include/print metadata.")
@click.pass_context
async def cli(ctx, host, port, auth, metadata):
    """The MoaT Key-Value subsystem.

    All commands (except 'server' and 'dump') connect to a MoaT-KV server.
    """
    obj = ctx.obj
    cfg = attrdict()
    if host is not None:
        cfg.host = host
    if port is not None:
        cfg.port = port

    if auth is not None:
        cfg.auth = gen_auth(auth)
        if obj.DEBUG:
            cfg.auth._DEBUG = True

    cfg = combine_dict(attrdict(kv=attrdict(conn=cfg)), obj.cfg, cls=attrdict)

    obj.meta = 3 if metadata else False

    try:
        if ctx.invoked_subcommand in {None, "server", "dump"}:
            obj.client = NullObj(RuntimeError("Not a client command"))
        else:
            obj.client = await client_scope(**cfg.kv)
    except OSError as exc:
        obj.client = NullObj(exc)
    else:
        logger.debug("Connected.")
