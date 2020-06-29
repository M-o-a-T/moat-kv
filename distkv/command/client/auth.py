# command line interface

import asyncclick as click

from distkv.util import split_one, NotGiven, yprint, Path
from distkv.auth import loader, gen_auth

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage authorization")  # pylint: disable=undefined-variable
@click.option("-m", "--method", default=None, help="Affect/use this auth method")
@click.pass_obj
async def cli(obj, method):
    """Manage authorization. Usage: … auth METHOD command…. Use '.' for 'all methods'."""
    a = await obj.client._request(action="auth_info")
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
        action="enum_internal",
        path=Path("auth"),
        iter=False,
        with_data=False,
        empty=True,
        nchain=0,
    )
    for r in res.result:
        print(r)
        yield r
    pass


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
                action="auth_list", typ=auth, kind=kind, ident=ident, iter=False, nchain=nchain
            )
            yield res
        else:
            async with obj.client._stream(
                action="auth_list", typ=auth, kind=kind, nchain=nchain
            ) as res:
                async for r in res:
                    yield r


@cli.command("list")
@click.pass_obj
async def list_(obj):
    """List known auth methods"""
    async for auth in enum_auth(obj):
        print(auth, file=obj.stdout)


@cli.command()
@click.option("-s", "--switch", is_flag=True, help="Switch to a different auth method")
@click.pass_obj
async def init(obj, switch):
    """Setup authorization"""
    if obj.auth_current is not None and not switch:
        raise click.UsageError("Authentication is already set up")

    await obj.client._request(action="set_auth_typ", typ=obj.auth)
    if obj.debug >= 0:
        if obj.auth:
            print("Authorization switched to", obj.auth, file=obj.stdout)
        else:
            print("Authorization turned off.", file=obj.stdout)


@cli.group()
async def user():
    """Manage users."""
    pass


@user.command("list")
@click.option(
    "-v", "--verbose", is_flag=True, help="Print complete results. Default: just the names"
)
@click.pass_obj  # pylint: disable=function-redefined
async def list_user(obj, verbose):
    """List all users (raw data)."""
    async for r in enum_typ(obj, nchain=obj.meta):
        if obj.meta or verbose:
            if obj.debug < 2:
                del r["seq"]
                del r["tock"]
            yprint(r, stream=obj.stdout)
        else:
            print(r.ident, file=obj.stdout)


@user.command()
@click.argument("ident", nargs=1)
@click.pass_obj
async def get(obj, ident):
    """Retrieve a user (processed)."""
    lv = loader(await one_auth(obj), "user", make=True, server=False)
    if obj._DEBUG:
        lv._length = 16

    u = await lv.recv(obj.client, ident, _initial=False)
    yprint(u.export(), stream=obj.stdout)


@user.command()
@click.argument("args", nargs=-1)
@click.pass_obj
async def add(obj, args):
    """Add a user."""
    await add_mod_user(obj, args, None)


@user.command()
@click.option("-n", "--new", is_flag=True, help="New: ignore previous content")
@click.argument("ident", nargs=1)
@click.argument("type", nargs=1)
@click.argument("key", nargs=1)
@click.argument("args", nargs=-1)
@click.pass_obj
async def param(obj, new, ident, type, key, args):  # pylint: disable=redefined-builtin
    """Set user parameters for auth, conversion, etc."""
    auth = await one_auth(obj)
    u = loader(auth, "user", make=True, server=False)
    if obj._DEBUG:
        u._length = 16
    # ou = await u.recv(obj.client, ident, _initial=False)  # unused
    res = await obj.client._request(
        action="get_internal", path=("auth", auth, "user", ident, type), iter=False, nchain=3
    )

    kw = res.get("value", NotGiven)
    if new or kw is NotGiven:
        kw = {}
        res.chain = None
    if key == "-":
        if args:
            raise click.UsageError("You can't set params when deleting")
        res = await obj.client._request(
            action="delete_internal",
            path=("auth", auth, "user", ident, type),
            iter=False,
            chain=res.chain,
        )

    else:
        kw["key"] = key

        for a in args:
            split_one(a, kw)

        res = await obj.client._request(
            action="set_internal",
            path=("auth", auth, "user", ident, type),
            iter=False,
            chain=res.chain,
            value=kw,
        )
    if obj.meta:
        # res.ident = ident
        # res.type = type
        yprint(res, stream=obj.stdout)
    else:
        print(ident, type, key, file=obj.stdout)


@user.command()
@click.argument("ident", nargs=1)
@click.argument("args", nargs=-1)
@click.pass_obj
async def mod(obj, ident, args):
    """Change a user."""
    await add_mod_user(obj, args, ident)


async def add_mod_user(obj, args, modify):
    auth = await one_auth(obj)
    u = loader(auth, "user", make=True, server=False)
    if obj._DEBUG:
        u._length = 16
    if modify:
        ou = await u.recv(obj.client, modify, _initial=False)
        kw = ou.export()
    else:
        kw = {}
    for a in args:
        split_one(a, kw)

    u = u.build(kw, _initial=False)
    if modify is None or u.ident != modify:
        u._chain = None  # new user
    else:
        u._chain = ou._chain
    res = await u.send(obj.client)
    if obj.meta:
        res.ident = u.ident
        yprint(res, stream=obj.stdout)
    else:
        print(u.ident, file=obj.stdout)


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
    user = gen_auth(auth)  # pylint: disable=redefined-outer-name
    await user.auth(obj.client)
    if obj.debug > 0:
        print("OK.", file=obj.stdout)
