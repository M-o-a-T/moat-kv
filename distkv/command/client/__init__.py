# command line interface

import asyncclick as click

from distkv.util import attrdict, combine_dict, load_subgroup
from distkv.client import open_client
from distkv.default import CFG
from distkv.auth import gen_auth

import logging

logger = logging.getLogger(__name__)


class NullObj:
    """
    This helper defers raising an exception until one of its attributes is
    actually accessed.
    """

    def __init__(self, exc):
        import pdb;pdb.set_trace()
        self._exc = exc

    def __call__(self, *a, **kw):
        raise self._exc

    def __await__(self):
        raise self._exc

    def __getattr__(self, k):
        if k[0] == "_" and k != "_request":
            return object.__getattribute__(self, k)
        raise self._exc


@load_subgroup(plugin="client")  # pylint: disable=undefined-variable
@click.option("-h", "--host", default=None, help=f"Host to use. Default: {CFG.connect.host}")
@click.option(
    "-p", "--port", type=int, default=None, help=f"Port to use. Default: {CFG.connect.port}"
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
    """Talk to a DistKV server."""
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

    cfg = combine_dict(attrdict(connect=cfg), obj.cfg, cls=attrdict)

    obj.meta = 3 if metadata else False

    try:
        obj.client = await ctx.with_async_resource(open_client(**cfg))
    except OSError as exc:
        obj.client = NullObj(exc)
    else:
        logger.debug("Connected.")
