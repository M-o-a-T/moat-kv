# command line interface

import os
import sys
import asyncclick as click
import json

from distkv.util import (
    attrdict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.auth import loader, gen_auth
from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage authorization")
@click.option("-m", "--method", default=None, help="Affect/use this auth method")
@click.pass_obj
async def cli(obj, method):
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


@cli.command()
@click.pass_obj
async def list(obj):
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
@click.pass_obj
async def user(obj):
    """Manage users."""
    pass


@user.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print complete results. Default: just the names",
)
@click.pass_obj
async def list(obj, verbose):
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

    u = await lv.recv(obj.client, ident)
    yprint(u.export(), stream=obj.stdout)


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
    user = gen_auth(auth)
    await user.auth(obj.client)
    if obj.debug > 0:
        print("OK.", file=obj.stdout)
