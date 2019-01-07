# command line interface

import sys
import trio_click as click
from pprint import pprint

from .util import attrdict, combine_dict, PathLongener
from .client import open_client, StreamReply
from .default import CFG, PORT
from .server import Server
from .model import Entry

import trio_click as click

import logging
logger = logging.getLogger(__name__)

class _NotGiven:
    pass
_NotGiven = _NotGiven()

def cmd():
    try:
        main(standalone_mode=False)
    except BaseException as exc:
        raise
        print(exc)
        sys.exit(1)

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
@click.argument("args", nargs=-1)
async def pdb(args):
    import pdb;pdb.set_trace()
    if not args:
        return
    return await main.main(args)

@main.command()
@click.option("-h","--host", default=None, help="Address to bind to. Default: %s" % (CFG.server.host))
@click.option("-p","--port", type=int, default=None, help="Port to bind to. Default: %d" % (CFG.server.port,))
@click.option("-l","--load", type=click.File('rb'), default=None, help="Event log to preload.")
@click.option("-s","--save", type=click.File('wb'), default=None, help="Event log to write to.")
@click.option("-i","--init", default=None, help="Initial value to set the root to. Use only when setting up a cluster for the first time!")
@click.option("-e", "--eval", is_flag=True, help="The 'init' value shall be evaluated.")
@click.argument("name", nargs=1)
@click.pass_context
async def run(ctx, name, host, port, load, save, init, eval):
    obj = ctx.obj
    print("Start run.")
    if host is not None:
        obj.cfg.server.host = host
    if port is not None:
        obj.cfg.server.port = port

    kw = {}
    if eval:
        kw['init'] = __builtins__['eval'](init)
    elif init == '-':
        kw['init'] = None
    elif init is not None:
        kw['init'] = init

    s = Server(name, cfg=obj.cfg, **kw)
    if load is None:
        await s.load(stream=load, local=True)
    await s.serve(obj.cfg, log_stream=save)


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
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-d", "--as-dict", default=None, help="YAML: structure as dictionary. The argument is the key to use for values. Default: return as list")
@click.option("-v", "--verbose", is_flag=True, help="Print the complete result. Default: just the value")
@click.option("-r", "--recursive", is_flag=True, help="Read a complete subtree")
@click.argument("path", nargs=-1)
@click.pass_context
async def get(ctx, path, chain, yaml, verbose, recursive, as_dict):
    """Read a DistKV value"""
    obj = ctx.obj
    if verbose and yaml:
        raise click.UsageError("'verbose' and 'yaml' are mutually exclusive")
    if recursive:
        if verbose and yaml:
            raise click.UsageError("'verbose' does not work in recursive YAML mode")
        res = await obj.client.request(action="get_tree", path=path, iter=True, nchain=chain)
        pl = PathLongener(path)
        y = {} if as_dict is not None else []
        async for r in res:
            pl(r)
            if yaml:
                if as_dict is not None:
                    yy = y
                    for p in r.path:
                        yy = yy.setdefault(p,{})
                        yy[as_dict] = r.value
                else:
                    yr = {'path': r.path, 'value': r.value}
                    if 'chain' in r:
                        yr['chain'] = r.chain
                    y.append(yr)
            else:
                if verbose:
                    pprint(r)
                else:
                    print("%s: %s" % (' '.join(r.path), repr(r.value)))
        if yaml:
            import yaml
            print(yaml.safe_dump(y))
        return
    res = await obj.client.request(action="get_value", path=path, iter=False, nchain=chain)
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
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.option("-p", "--prev", default=_NotGiven, help="Previous value. Deprecated; use 'last'")
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-y", "--yaml", is_flag=True, help="Print result as YAML. Default: Python.")
@click.argument("path", nargs=-1)
@click.pass_context
async def set(ctx, path, value, eval, chain, prev, last, yaml):
    """Set a DistKV value"""
    obj = ctx.obj
    if eval:
        value = __builtins__['eval'](value)
    args = {}
    if prev is not _NotGiven:
        if eval:
            prev = __builtins__['eval'](prev)
        args['prev'] = prev
    if last:
        if last[1] == '-':
            args['chain'] = None
        else:
            args['chain'] = {'node': last[0], 'tick': int(last[1])}

    res = await obj.client.request(action="set_value", value=value, path=path, iter=False, nchain=chain, **args)
    if yaml:
        import yaml
        print(yaml.safe_dump(res))
    elif chain:
        pprint(res)


@client.command()
@click.argument("path", nargs=-1)
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.pass_context
async def delete(ctx, path, chain, recursive):
    obj = ctx.obj
    res = await obj.client.request(action="delete_tree" if recursive else "delete_value", path=path, nchain=chain)
    if isinstance(res, StreamReply):
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            pprint(r)
    else:
        pprint(res)


@client.command()
@click.option("-c", "--chain", type=int, default=None, help="Length of change list to return. Default: 0")
@click.option("-s", "--state", is_flag=True, help="Also get the current state.")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.argument("path", nargs=-1)
@click.pass_context
async def watch(ctx, path, chain, yaml, state):
    """Watch a DistKV subtree"""
    if yaml:
        import yaml
    obj = ctx.obj
    res = await obj.client.request(action="watch", path=path, iter=True, nchain=chain, state=state)
    pl = PathLongener(path)
    async for r in res:
        pl(r)
        del r['seq']
        if yaml:
            print(yaml.safe_dump(r))
        else:
            pprint(r)

@client.command()
@click.option("-l", "--local", is_flag=True, help="Load locally, don't broadcast")
@click.option("-f", "--force", is_flag=True, help="Overwrite existing values")
@click.option("-i", "--infile", type=click.File('rb'), help="Print as YAML. Default: Python.")
@click.argument("path", nargs=-1)
@click.pass_context
async def update(ctx, path, infile, loca, force):
    """Send a list of updates to a DistKV subtree"""
    if local and force:
        raise click.UsageError("'local' and 'force' are mutually exclusive")

    obj = ctx.obj
    ps = PathShortener()
    async with MsgReader() as reader:
        with obj.client.stream(action="update", path=path, iter=False, force=force, local=local) as sender:
            async for r in res:
                ps(r)
                await sender(r)

    print(sender.result)

