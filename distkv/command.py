# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

from .util import (
    attrdict,
    combine_dict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from .client import open_client, StreamedRequest
from .default import CFG
from .server import Server
from .auth import loader, gen_auth
from .exceptions import ClientError

import logging

logger = logging.getLogger(__name__)


def cmd():
    try:
        main(standalone_mode=False)
    except click.exceptions.UsageError as exc:
        try:
            s = str(exc)
        except TypeError:
            logger.exception(repr(exc), exc_info=exc)
        else:
            print(s, file=sys.stderr)
    except click.exceptions.Abort:
        print("Aborted.", file=sys.stderr)
        pass
    except ClientError as err:
        print(type(err).__name__ + ":", *err.args, file=sys.stderr)
        sys.exit(1)
    except BaseException as exc:
        raise
        print(exc)
        sys.exit(1)


@click.group()
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Enable debugging. Use twice for more verbosity.",
)
@click.option(
    "-q", "--quiet", count=True, help="Disable debugging. Opposite of '--verbose'."
)
@click.option(
    "-D", "--debug", is_flag=True, help="Enable debug speed-ups (smaller keys etc)."
)
@click.option("-c", "--cfg", type=click.File("r"), default=None)
@click.pass_context
async def main(ctx, verbose, quiet, debug, cfg):
    ctx.ensure_object(attrdict)
    ctx.obj.debug = verbose - quiet
    ctx.obj._DEBUG = debug
    logging.basicConfig(
        level=logging.DEBUG
        if verbose > 2
        else logging.INFO
        if verbose > 1
        else logging.WARNING
        if verbose > 0
        else logging.ERROR
    )
    if cfg:
        logger.debug("Loading %s", cfg)
        import yaml

        ctx.obj.cfg = combine_dict(yaml.safe_load(cfg), CFG)
        cfg.close()
    else:
        ctx.obj.cfg = CFG


@main.command()
@click.argument("args", nargs=-1)
async def pdb(args):  # safe
    import pdb

    pdb.set_trace()  # safe
    if not args:
        return
    return await main.main(args)


@main.command()
@click.option(
    "-h",
    "--host",
    default=None,
    help="Address to bind to. Default: %s" % (CFG.server.host),
)
@click.option(
    "-p",
    "--port",
    type=int,
    default=None,
    help="Port to bind to. Default: %d" % (CFG.server.port,),
)
@click.option(
    "-l",
    "--load",
    type=click.Path(readable=True, exists=True, allow_dash=False),
    default=None,
    help="Event log to preload.",
)
@click.option(
    "-s",
    "--save",
    type=click.Path(writable=True, allow_dash=False),
    default=None,
    help="Event log to write to.",
)
@click.option(
    "-i",
    "--init",
    default=None,
    help="Initial value to set the root to. Use only when setting up "
    "a cluster for the first time!",
)
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
        kw["init"] = __builtins__["eval"](init)
    elif init == "-":
        kw["init"] = None
    elif init is not None:
        kw["init"] = init

    class RunMsg:
        def started(self, x=None):
            print("Running.")

    s = Server(name, cfg=obj.cfg, **kw)
    if load is not None:
        await s.load(path=load, local=True)
    await s.serve(log_path=save, task_status=RunMsg())


@main.group()
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
async def client(ctx, host, port, auth):
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

    obj.client = await ctx.enter_async_context(open_client(host, port, **kw))
    logger.debug("Connected.")


@client.command()
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="YAML: structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-m",
    "--maxdepth",
    type=int,
    default=None,
    help="Limit recursion depth. Default: whole tree",
)
@click.option(
    "-M",
    "--mindepth",
    type=int,
    default=None,
    help="Starting depth. Default: whole tree",
)
@click.option("-r", "--recursive", is_flag=True, help="Read a complete subtree")
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, chain, yaml, verbose, recursive, as_dict, maxdepth, mindepth):
    """Read a DistKV value"""
    if recursive:
        kw = {}
        if maxdepth is not None:
            kw["max_depth"] = maxdepth
        if mindepth is not None:
            kw["min_depth"] = mindepth
        res = await obj.client.get_tree(*path, nchain=chain, **kw)
        pl = PathLongener(path)
        y = {} if as_dict is not None else []
        async for r in res:
            pl(r)
            r.pop("seq", None)
            if yaml:
                if as_dict is not None:
                    yy = y
                    for p in r.pop("path"):
                        yy = yy.setdefault(p, {})
                    if "chain" in r:
                        yy["chain"] = r.chain
                    yy[as_dict] = r.pop("value")
                    if verbose:
                        yy.update(r)
                else:
                    if verbose:
                        y.append(r)
                    else:
                        yr = {"path": r.path, "value": r.value}
                        if "chain" in r:
                            yr["chain"] = r.chain
                        y.append(yr)
            else:
                if verbose:
                    pprint(r)
                else:
                    print("%s: %s" % (" ".join(r.path), repr(r.value)))
        if yaml:
            import yaml

            print(yaml.safe_dump(y, default_flow_style=False))
        return
    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    res = await obj.client.get(*path, nchain=chain)
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
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.option(
    "-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option(
    "-y", "--yaml", is_flag=True, help="Print result as YAML. Default: Python."
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, value, eval, chain, prev, last, yaml):
    """Set a DistKV value"""
    if eval:
        value = __builtins__["eval"](value)
    args = {}
    if prev is not NotGiven:
        if eval:
            prev = __builtins__["eval"](prev)
        args["prev"] = prev
    if last:
        if last[1] == "-":
            args["chain"] = None
        else:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client.set(*path, value=value, nchain=chain, **args)
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False))
    elif chain:
        pprint(res)


@client.command()
@click.argument("path", nargs=-1)
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.pass_obj
async def delete(obj, path, chain, recursive):
    """Delete a node."""
    res = await obj.client._request(
        action="delete_tree" if recursive else "delete_value", path=path, nchain=chain
    )
    if isinstance(res, StreamedRequest):
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            pprint(r)
    else:
        pprint(res)


@client.command()
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.option("-s", "--state", is_flag=True, help="Also get the current state.")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def watch(obj, path, chain, yaml, state):
    """Watch a DistKV subtree"""
    if yaml:
        import yaml
    async with obj.client.watch(*path, nchain=chain, fetch=state) as res:
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            del r["seq"]
            if yaml:
                print(yaml.safe_dump(r, default_flow_style=False))
            else:
                pprint(r)


@client.command()
@click.option("-l", "--local", is_flag=True, help="Load locally, don't broadcast")
@click.option("-f", "--force", is_flag=True, help="Overwrite existing values")
@click.option(
    "-i", "--infile", type=click.File("rb"), help="Print as YAML. Default: Python."
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def update(obj, path, infile, local, force):
    """Send a list of updates to a DistKV subtree"""
    if local and force:
        raise click.UsageError("'local' and 'force' are mutually exclusive")

    ps = PathShortener()
    async with MsgReader(path=path) as reader:
        with obj.client._stream(
            action="update", path=path, force=force, local=local
        ) as sender:
            async for r in reader:
                ps(r)
                await sender.send(r)

    print(sender.result)


@client.group()
@click.option("-m", "--method", default=None, help="Affect/use this auth method")
@click.pass_obj
async def auth(obj, method):
    """Manage authorization. Usage: … auth METHOD command…. Use '.' for 'all methods'."""
    a = await obj.client.get(None, "auth")
    a = a.get("value", None)
    if a is not None:
        a = a["current"]
    obj.auth_current = a
    if method is None:
        obj.auth = a or (await one_auth(obj))
    else:
        if method == "-":
            method = None
        obj.auth = method


async def enum_auth(obj):
    """List all configured auth types."""
    if obj.get("auth", None) is not None:
        yield obj.auth
        return
    # TODO create a method for this
    res = await obj.client._request(
        action="get_tree",
        path=(None, "auth"),
        iter=True,
        nchain=0,
        min_depth=1,
        max_depth=1,
    )
    async for r in res:
        yield r.path[-1]


async def one_auth(obj):
    """Return the current auth method (from the command line or as used by the server)."""
    if obj.get("auth", None) is not None:
        return obj.auth
    auth = None
    async for a in enum_auth(obj):
        if auth is not None:
            raise click.UsageError("You need to set the auth method")
        auth = a
    if auth is None:
        raise click.UsageError("You need to set the auth method")
    return auth


async def enum_typ(obj, kind="user", ident=None, nchain=0):
    """List all known auth entries of a kind."""
    async for auth in enum_auth(obj):
        if ident is not None:
            res = await obj.client._request(
                action="auth_list",
                typ=auth,
                kind=kind,
                ident=ident,
                iter=False,
                nchain=nchain,
            )
            yield res
        else:
            async with obj.client._stream(
                action="auth_list", typ=auth, kind=kind, nchain=nchain
            ) as res:
                async for r in res:
                    yield r


@auth.command()
@click.pass_obj
async def list(obj):
    """List known auth methods"""
    async for auth in enum_auth(obj):
        print(auth)


@auth.command()
@click.option("-s", "--switch", is_flag=True, help="Switch to a different auth method")
@click.pass_obj
async def init(obj, switch):
    """Setup authorization"""
    if obj.auth_current is not None and not switch:
        raise click.UsageError("Authentication is already set up")

    await obj.client._request(action="set_auth_typ", typ=obj.auth)
    if obj.debug >= 0:
        if obj.auth:
            print("Authorization switched to", obj.auth)
        else:
            print("Authorization turned off.")


@auth.group()
@click.pass_obj
async def user(obj):
    """Manage users."""
    pass


@user.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.pass_obj
async def list(obj, yaml, chain):
    """List all users (raw data)."""
    if yaml:
        import yaml
    async for r in enum_typ(obj, nchain=chain):
        if obj.debug < 2:
            del r["seq"]
            del r["tock"]
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
    """Retrieve a user (processed)."""
    lv = loader(await one_auth(obj), "user", make=True, server=False)
    if obj._DEBUG:
        lv._length = 16

    u = await lv.recv(obj.client, ident)
    pprint(u.export())


@user.command()
@click.option("-a", "--add", multiple=True, help="additional non-method-specific data")
@click.argument("args", nargs=-1)
@click.pass_obj
async def add(obj, args, add):
    """Add a user."""
    await add_mod_user(obj, args, None, add)


@user.command()
@click.option("-a", "--add", multiple=True, help="additional non-method-specific data")
@click.argument("ident", nargs=1)
@click.argument("args", nargs=-1)
@click.pass_obj
async def mod(obj, ident, args, add):
    """Change a user."""
    await add_mod_user(obj, args, ident, add)


async def add_mod_user(obj, args, modify, add):
    auth = await one_auth(obj)
    u = loader(auth, "user", make=True, server=False)
    if obj._DEBUG:
        u._length = 16
    if modify:
        ou = await u.recv(obj.client, modify)
        kw = ou.export()
    else:
        kw = {}
    for a in args:
        split_one(a, kw)
    if add:
        ax = kw.setdefault("aux", {})
        for a in add:
            split_one(a, ax)

    u = u.build(kw)
    if modify is None or u.ident != modify:
        u._chain = None  # new user
    else:
        u._chain = ou._chain
    await u.send(obj.client)
    print("Added" if u._chain is None else "Modified", u.ident)


@user.command(name="auth")
@click.option(
    "-a",
    "--auth",
    type=str,
    default="root",
    help="Auth params. =file or 'type param=value…' Default: root",
)
@click.pass_obj
async def auth_(obj, auth):
    """Test user authorization."""
    user = gen_auth(auth)
    await user.auth(obj.client)
    if obj.debug >= 0:
        print("OK.")


@client.group()
@click.pass_obj
async def type(obj):
    """Manage types and type matches. Usage: … type …"""
    pass


@type.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the script here"
)
@click.option(
    "-S", "--schema", type=click.File(mode="w", lazy=True), help="Save the schema here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, chain, yaml, verbose, script, schema):
    """Read type checker information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal",
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if not verbose:
        res = res.value
    if yaml:
        import yaml

        if schema and res.get("schema", None) is not None:
            print(
                yaml.safe_dump(res.pop("schema"), default_flow_style=False), file=schema
            )
        print(yaml.safe_dump(res, default_flow_style=False), file=script or sys.stdout)
    else:
        if script:
            code = res.pop("code", None)
            if code is not None:
                print(code, file=script)
        if schema and res.get("schema", None) is not None:
            json.dump(res.pop("schema"), schema)
        pprint(res)


@type.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-g", "--good", multiple=True, help="Example for passing values")
@click.option("-b", "--bad", multiple=True, help="Example for failing values")
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the checking script"
)
@click.option(
    "-S", "--schema", type=click.File(mode="r"), help="File with the JSON schema"
)
@click.option("-y", "--yaml", is_flag=True, help="load everything from this file")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, good, bad, verbose, script, schema, yaml):
    """Write type checker information."""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    msg.setdefault("good", [])
    msg.setdefault("bad", [])
    for x in good:
        msg["good"].append(eval(x))
    for x in bad:
        msg["bad"].append(eval(x))
    if "code" not in msg:
        if not script:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to verify 'value'.")
            script = sys.stdin.read()
        else:
            script = script.read()
        msg["code"] = script
    elif script and not yaml:
        raise click.UsageError("Duplicate script parameter")
    if "schema" not in msg:
        if schema:
            if yaml:
                schema = yaml.safe_load(schema)
            else:
                schema = json.load(schema)
            msg["schema"] = schema
    elif schema:
        raise click.UsageError("Duplicate schema parameter")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if verbose:
        print(res.tock)


@type.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-t", "--type", multiple=True, help="Type to link to. Multiple for subytpes."
)
@click.option("-d", "--delete", help="Use to delete this mapping.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def match(obj, path, type, delete, verbose):
    """Match a type to a path (read, if no type given)"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if type and delete:
        raise click.UsageError("You can't both set and delete a path.")

    if delete:
        await obj.client._request(action="delete_internal", path=("type",) + path)
        return

    msg = {"type": type}
    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("match",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if type or delete:
        print(res.tock)
    elif verbose:
        pprint(res)
    else:
        print(" ".join(res.type))


@client.group()
@click.pass_obj
async def codec(obj):
    """Manage codecs and converters. Usage: … codec …"""
    pass


@codec.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-e", "--encode", type=click.File(mode="w", lazy=True), help="Save the encoder here"
)
@click.option(
    "-d", "--decode", type=click.File(mode="w", lazy=True), help="Save the decoder here"
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the data here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, chain, yaml, verbose, script, encode, decode):
    """Read type information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal",
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if encode and res.get("encode", None) is not None:
        encode.write(res.pop("encode"))
    if decode and res.get("decode", None) is not None:
        decode.write(res.pop("decode"))

    if not verbose:
        res = res.value
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False), file=script or sys.stdout)
    else:
        pprint(res)


@codec.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-e", "--encode", type=click.File(mode="r"), help="File with the encoder")
@click.option("-d", "--decode", type=click.File(mode="r"), help="File with the decoder")
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the rest")
@click.option("-y", "--yaml", is_flag=True, help="load everything from this file")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, verbose, encode, decode, script, yaml):
    """Save codec information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    if "encode" not in msg:
        if not encode:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to encode 'value'.")
            encode = sys.stdin.read()
        else:
            encode = encode.read()
        msg["encode"] = encode
    elif encode and not yaml:
        raise click.UsageError("Duplicate encode parameter")
    if "decode" not in msg:
        if not decode:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to decode 'value'.")
            decode = sys.stdin.read()
        else:
            decode = decode.read()
        msg["decode"] = decode
    elif decode and not yaml:
        raise click.UsageError("Duplicate decode parameter")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("codec",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if verbose:
        pprint(res)
    elif obj.verbose:
        print(res.tock)


@codec.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-c", "--codec", multiple=True, help="Codec to link to. Multiple for hierarchical."
)
@click.option("-d", "--delete", help="Use to delete this converter.")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def convert(obj, path, codec, name, delete, verbose):
    """Match a codec to a path (read, if no codec given)"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if codec and delete:
        raise click.UsageError("You can't both set and delete a path.")

    if delete:
        res = await obj.client._request(
            action="delete_internal", path=("conv", name) + path
        )
    else:
        msg = {"codec": codec}
        res = await obj.client._request(
            action="set_internal",
            value=msg,
            path=("conv", name) + path,
            iter=False,
            nchain=3 if verbose else 0,
        )
    if verbose:
        pprint(res)
    elif type or delete:
        print(res.tock)
    else:
        print(" ".join(res.type))
