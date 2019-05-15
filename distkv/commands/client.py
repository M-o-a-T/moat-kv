# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json
from functools import partial

from distkv.util import (
    attrdict,
    combine_dict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.client import open_client, StreamedRequest
from distkv.command import Loader
from distkv.default import CFG
from distkv.server import Server
from distkv.auth import loader, gen_auth
from distkv.exceptions import ClientError

import logging

logger = logging.getLogger(__name__)


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
        if k[0] == '_':
            return object.__getattr__(self, k)
        raise self._exc


@main.group(cls=partial(Loader,__file__,"client"))
@click.option(
    "-h", "--host", default=None, help="Host to use. Default: %s" % (CFG.server.host,)
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=None,
    help="Port to use. Default: %d" % (CFG.server.port,),
)
@click.option(
    "-a",
    "--auth",
    type=str,
    default=None,
    help="Auth params. =file or 'type param=value…' Default: _anon",
)
@click.pass_context
async def cli(ctx, host, port, auth):
    """Talk to a DistKV server."""
    obj = ctx.obj
    if host is None:
        host = obj.cfg.server.host
    if port is None:
        port = obj.cfg.server.port

    kw = {}
    if auth is not None:
        kw["auth"] = gen_auth(auth)
        if obj._DEBUG:
            kw["auth"]._DEBUG = True

    try:
        obj.client = await ctx.enter_async_context(open_client(host, port, **kw))
    except OSError as exc:
        obj.client = NullObj(exc)
    else:
        logger.debug("Connected.")

