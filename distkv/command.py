# command line interface

import trio_click as click
from pprint import pprint

from .util import attrdict, combine_dict
from .client import open_client
from .default import CFG, PORT
from .server import Server
from .model import Entry

import trio_click as click

import logging
logger = logging.getLogger(__name__)


@click.group()
@click.option("-v","--verbose", count=True, help="Enable debugging. Use twice for more verbosity.")
@click.option("-q","--quiet", count=True, help="Disable debugging. Opposite of '--verbose'.")
@click.option("-c","--cfg", type=click.File('r'), default=None)
@click.pass_context
async def main(ctx, verbose, quiet, cfg):
    ctx.ensure_object(attrdict)
    ctx.obj.debug = verbose - quiet
    logging.basicConfig(level=logging.DEBUG if verbose>2 else
                              logging.INFO if verbose>1 else
                              logging.WARNING if verbose>0 else
                              logging.ERROR)
    if cfg:
        logger.debug("Loading %s",cfg)
        import yaml
        ctx.obj.cfg = combine_dict(yaml.safe_load(cfg), CFG)
        cfg.close()
    else:
        ctx.obj.cfg = CFG

@main.command()
@click.option("-h","--host", default=None, help="Address to bind to. Default: %s" % (CFG.server.host))
@click.option("-p","--port", type=int, default=None, help="Port to bind to. Default: %d" % (CFG.server.port,))
@click.option("-l","--load", type=click.File('rb'), default=None, help="Event log to load. Default: %d" % (CFG.server.port,))
@click.argument("name", nargs=1)
@click.pass_context
async def run(ctx, name, host, port, load):
    obj = ctx.obj
    print("Start run.")
    if host is None:
        host = obj.cfg.server.host
    if port is None:
        port = obj.cfg.server.port

    obj.root = Entry("ROOT", None)
    if load:
        await process_events(obj.root, load)
    s = Server(name, obj.root, host, port)
    await s.serve(obj.cfg)


@main.group()
@click.option("-h","--host", default=None, help="Host to use. Default: %s" % (CFG.server.host,))
@click.option("-p","--port", type=int, default=None, help="Port to use. Default: %d" % (CFG.server.port,))
@click.pass_context
async def client(ctx,host,port):
    obj = ctx.obj
    if host is None:
        host = obj.cfg.server.host
    if port is None:
        port = obj.cfg.server.port
    obj.client = await ctx.enter_async_context(open_client(host, port))
    logger.debug("Connected.")


@client.command()
@click.option("-d", "--depth", default=0, help="Length of change list to return. Default: None")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-v", "--verbose", is_flag=True, help="Print the complete result. Default: just the value")
@click.argument("path", nargs=-1)
@click.pass_context
async def get(ctx, path, depth, yaml, verbose):
    """Read a DistKV value"""
    obj = ctx.obj
    res = await obj.client.request(action="get_value", path=path, iter=False, depth=depth)
    if not verbose:
        res = res.value
    if yaml:
        import yaml
        print(yaml.safe_dump(res))
    else:
        pprint(res)


@client.command()
@click.option("-v", "--value", help="Value to set. Mandatory.")
@click.option("-e", "--eval", is_flag=True, help="The value shall be evaluated.")
@click.argument("path", nargs=-1)
@click.pass_context
async def set(ctx, path, value, eval):
    """Set a DistKV value"""
    obj = ctx.obj
    if eval:
        value = __builtins__['eval'](value)
    res = await obj.client.request(action="set_value", value=value, path=path, iter=False)


@client.command()
@click.argument("path", nargs=-1)
@click.pass_context
async def delete(ctx, path):
    obj = ctx.obj
    res = await obj.client.request(action="del_value", path=path)

