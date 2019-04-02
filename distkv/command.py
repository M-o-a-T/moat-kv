# command line interface

import sys
import trio_click as click
from pprint import pprint

from .util import attrdict, combine_dict, PathLongener, acount
from .client import open_client, StreamedRequest
from .default import CFG, PORT
from .server import Server
from .auth import loader, gen_auth
from .model import Entry
from .exceptions import ClientError

import trio_click as click

import logging
logger = logging.getLogger(__name__)

class _NotGiven:
    pass
_NotGiven = _NotGiven()

def cmd():
    try:
        main(standalone_mode=False)
    except click.exceptions.UsageError as exc:
        print(str(exc), file=sys.stderr)
    except click.exceptions.Abort:
        print("Aborted.", file=sys.stderr)
        pass
    except ClientError as err:
        print(type(err).__name__+':', *err.args, file=sys.stderr)
        sys.exit(1)
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
async def pdb(args):  # safe
    import pdb;pdb.set_trace()  # safe
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
@click.pass_obj
async def run(obj, name, host, port, load, save, init, eval):
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

    class RunMsg:
        def started(self, x=None):
            print("Running.")
    s = Server(name, cfg=obj.cfg, **kw)
    if load is not None:
        await s.load(stream=load, local=True)
    await s.serve(log_stream=save, task_status=RunMsg())


@main.group()
@click.option("-h","--host", default=None, help="Host to use. Default: %s" % (CFG.server.host,))
@click.option("-p","--port", type=int, default=None, help="Port to use. Default: %d" % (CFG.server.port,))
@click.option("-a","--auth", type=str, default=None, help="Auth params. =file or 'type param=value…' Default: _anon")
@click.pass_context
async def client(ctx,host,port,auth):
    obj = ctx.obj
    if host is None:
        host = obj.cfg.server.host
    if port is None:
        port = obj.cfg.server.port

    kw = {}
    if auth is not None:
        kw['auth'] = gen_auth(auth)

    obj.client = await ctx.enter_async_context(open_client(host, port, **kw))
    logger.debug("Connected.")


@client.command()
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-d", "--as-dict", default=None, help="YAML: structure as dictionary. The argument is the key to use for values. Default: return as list")
@click.option("-v", "--verbose", is_flag=True, help="Print the complete result. Default: just the value")
@click.option("-m", "--maxdepth", type=int, default=None, help="Limit recursion depth. Default: whole tree")
@click.option("-M", "--mindepth", type=int, default=None, help="Starting depth. Default: whole tree")
@click.option("-r", "--recursive", is_flag=True, help="Read a complete subtree")
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, chain, yaml, verbose, recursive, as_dict, maxdepth, mindepth):
    """Read a DistKV value"""
    if recursive:
        kw = {}
        if maxdepth is not None:
            kw['maxdepth'] = maxdepth
        if mindepth is not None:
            kw['mindepth'] = mindepth
        res = await obj.client.request(action="get_tree", path=path, iter=True, nchain=chain, **kw)
        pl = PathLongener(path)
        y = {} if as_dict is not None else []
        async for r in res:
            pl(r)
            if yaml:
                if as_dict is not None:
                    yy = y
                    for p in r.pop('path'):
                        yy = yy.setdefault(p,{})
                    if 'chain' in r:
                        yy['chain'] = r.chain
                    yy[as_dict] = r.pop('value')
                    if verbose:
                        yy.update(r)
                else:
                    if verbose:
                        y.append(r)
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
            print(yaml.safe_dump(y, default_flow_style=False))
        return
    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    res = await obj.client.request(action="get_value", path=path, iter=False, nchain=chain)
    if not verbose:
        res = res.value
    if yaml:
        import yaml
        print(yaml.safe_dump(res, default_flow_style=False))
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
@click.pass_obj
async def set(obj, path, value, eval, chain, prev, last, yaml):
    """Set a DistKV value"""
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
        print(yaml.safe_dump(res, default_flow_style=False))
    elif chain:
        pprint(res)


@client.command()
@click.argument("path", nargs=-1)
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.pass_obj
async def delete(obj, path, chain, recursive):
    """Delete a node."""
    res = await obj.client.request(action="delete_tree" if recursive else "delete_value", path=path, nchain=chain)
    if isinstance(res, StreamedRequest):
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
@click.pass_obj
async def watch(obj, path, chain, yaml, state):
    """Watch a DistKV subtree"""
    if yaml:
        import yaml
    res = await obj.client.request(action="watch", path=path, iter=True, nchain=chain, state=state)
    pl = PathLongener(path)
    async for r in res:
        pl(r)
        del r['seq']
        if yaml:
            print(yaml.safe_dump(r, default_flow_style=False))
        else:
            pprint(r)

@client.command()
@click.option("-l", "--local", is_flag=True, help="Load locally, don't broadcast")
@click.option("-f", "--force", is_flag=True, help="Overwrite existing values")
@click.option("-i", "--infile", type=click.File('rb'), help="Print as YAML. Default: Python.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def update(obj, path, infile, loca, force):
    """Send a list of updates to a DistKV subtree"""
    if local and force:
        raise click.UsageError("'local' and 'force' are mutually exclusive")

    ps = PathShortener()
    async with MsgReader() as reader:
        with obj.client.stream(action="update", path=path, force=force, local=local) as sender:
            async for r in res:
                ps(r)
                await sender.send(r)

    print(sender.result)


@client.group()
@click.option('-m',"--method", default=None, help="Affect this auth method")
@click.pass_obj
async def auth(obj, method):
    """Manage authorization. Usage: … auth METHOD command…. Use '.' for 'all methods'."""
    a = await obj.client.request(action="get_value", path=(None,'auth'))
    a = a.value
    if a is not None:
        a = a['current']
    obj.auth_current = a
    obj.auth = method or a or (await one_auth(obj))


async def enum_auth(obj):
    """List all configured auth types."""
    if obj.get('auth',None) is not None:
        yield obj.auth
        return
    res = await obj.client.request(action="get_tree", path=(None,'auth'), iter=True, nchain=0, mindepth=1,maxdepth=1)
    async for r in res:
        yield r.path[-1]


async def one_auth(obj):
    """Return the current auth method (from the command line or as used by the server)."""
    if obj.get('auth',None) is not None:
        return obj.auth
    auth = None
    async for a in enum_auth(obj):
        if auth is not None:
            raise click.UsageError("You need to set the auth method")
        auth = a
    if auth is None:
        raise click.UsageError("You need to set the auth method")
    return auth
            

async def enum_typ(obj, kind='user', ident=None, nchain=0):
    """List all known auth entries of a kind."""
    async for auth in enum_auth(obj):
        if ident is not None:
            res = await obj.client.request(action="auth_list", typ=auth, kind=kind, ident=ident, iter=False, nchain=nchain)
            yield res
        else:
            async with obj.client.stream(action="auth_list", typ=auth, kind=kind, nchain=nchain) as res:
                async for r in res:
                    yield r


@auth.command()
@click.pass_obj
async def list(obj):
    """List known auth methods"""
    async for auth in enum_auth(obj):
        print(auth)


@auth.command()
@click.option("-s","--switch",is_flag=True,help="Switch to a different auth method")
@click.pass_obj
async def init(obj,switch):
    """Setup authorization"""
    if obj.auth_current is not None and not switch:
        raise click.UsageError("Authentication is already set up")

    await obj.client.request(action="set_auth_typ", typ=obj.auth)
    if obj.debug >= 0:
        if obj.auth:
            print("Authorization switched to",obj.auth)
        else:
            print("Authorization turned off.")


@auth.group()
@click.pass_obj
async def user(obj):
    """Manage users."""
    pass

@user.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-c", "--chain", default=0, help="Length of change list to return. Default: 0")
@click.pass_obj
async def list(obj, yaml,chain):
    """List all users."""
    if yaml:
        import yaml
    async for r in enum_typ(obj,nchain=chain):
        if obj.debug < 2:
            del r['seq']
            del r['tock']
        if yaml:
            print(yaml.safe_dump(r, default_flow_style=False))
        elif obj.debug > 0:
            print(r)
        else:
            print(r.ident)

@user.command()
@click.argument("ident", nargs=1)
@click.pass_obj
async def get(obj, ident):
    """Retrieve a user."""
    l = loader(await one_auth(obj),'user', make=True, server=False)
    async for u in enum_typ(obj, ident=ident):
        u = await l.load(obj.client,u)
        print(u.export())


@user.command()
@click.argument("args", nargs=-1)
@click.pass_obj
async def add(obj, args):
    """Add a user."""
    await add_mod_user(obj, args, None)

@user.command()
@click.argument("ident", nargs=1)
@click.argument("args", nargs=-1)
@click.pass_obj
async def mod(obj, ident, args):
    """Change a user."""
    await add_mod_user(obj, args, ident)

async def add_mod_user(obj, args, modify):
    auth = await one_auth(obj)
    u = loader(auth,'user', make=True, server=False)
    chain=None
    if modify:
        ou = await u.recv(obj.client, modify)
        kw = await ou.export()
    else:
        kw = {}
    for a in args:
        if '=' in a:
            k,v = a.split('=',1)
            try:
                v = int(v)
            except ValueError:
                pass
            kw[k] = v
    u = u.build(kw)
    if modify is not None and u.ident != modify:
        chain = None  # new user
    res = await u.send(obj.client)
    print("Added" if chain is None else "Modified" ,u.ident)


@user.command(name="auth")
@click.option("-a","--auth", type=str, default="root", help="Auth params. =file or 'type param=value…' Default: root")
@click.pass_obj
async def auth_(obj, auth):
    """Test user authorization."""
    user = gen_auth(auth)
    await user.auth(obj.client)
    if obj.debug >= 0:
        print("OK.")

    
async def add_mod_user(obj, args, modify):
    auth = await one_auth(obj)
    u = loader(auth,'user', make=True, server=False)
    chain=None
    if modify:
        ou = await u.recv(obj.client, modify)
        kw = await ou.export()
    else:
        kw = {}
    for a in args:
        if '=' in a:
            k,v = a.split('=',1)
            try:
                v = int(v)
            except ValueError:
                pass
            kw[k] = v
    u = u.build(kw)
    if modify is not None and u.ident != modify:
        chain = None  # new user
    res = await u.send(obj.client)
    print("Added" if chain is None else "Modified" ,u.ident)


