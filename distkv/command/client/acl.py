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
from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)

ACL = set("rwdcxena")
# read, write, delete, create, access, enumerate


@main.group()
@click.pass_obj
async def cli(obj):
    """Manage ACLs. Usage: … acl …"""
    pass


@cli.command()
@click.pass_obj
async def list(obj):
    """List ACLs.
    """
    res = await obj.client._request(
        action="enum_internal", path=("acl",), iter=False, nchain=obj.meta
    )
    yprint(res, stream=obj.stdout)


@cli.command()
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def dump(obj, name, path, as_dict):
    """Dump a complete (or partial) ACL."""
    res = await obj.client._request(
        action="get_tree_internal", path=("acl", name) + path, iter=True
    )
    y = {}
    async for r in res:
        if as_dict is not None:
            yy = y
            for p in r.pop("path"):
                yy = yy.setdefault(p, {})
            if "chain" in r:
                yy["chain"] = r.chain
            yy[as_dict] = r.pop("value")
        else:
            y = {}
            y[r.path] = r.value
            yprint([y], stream=obj.stdout)

    if as_dict:
        yprint(y, stream=obj.stdout)


@cli.command()
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, name, path):
    """Read an ACL.
    
    This command does not test a path. Use "… acl test …" for that.
    """
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal", path=("acl", name) + path, iter=False, nchain=obj.meta
    )

    if not obj.meta:
        try:
            res = res.value
        except KeyError:
            if obj.debug:
                print("No value.", file=sys.stderr)
            return
    yprint(res, stream=obj.stdout)


@cli.command(name="set")
@click.option(
    "-a",
    "--acl",
    default="+x",
    help="The value to set. Start with '+' to add, '-' to remove rights.",
)
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set_(obj, acl, name, path):
    """Set or change an ACL."""

    if not path:
        raise click.UsageError("You need a non-empty path.")
    if len(acl) == 1 and acl in "+-":
        mode = acl[0]
        acl = acl[1:]
    else:
        mode = "x"
    acl = set(acl)

    if acl - ACL:
        raise click.UsageError(
            "You're trying to set an unknown ACL flag: %r" % (acl - ACL,)
        )

    res = await obj.client._request(
        action="get_internal",
        path=("acl", name) + path,
        iter=False,
        nchain=3 if obj.meta else 1,
    )
    ov = set(res.get("value", ""))
    if ov - ACL:
        print(
            "Warning: original ACL contains unknown: %r" % (ov - acl,), file=sys.stderr
        )

    if mode == "-" and not acl:
        res = await obj.client._request(
            action="delete_internal",
            path=("acl", name) + path,
            iter=False,
            chain=res.chain,
        )
        v = "-"

    else:
        if mode == "+":
            v = ov + acl
        elif mode == "-":
            v = ov - acl
        else:
            v = acl
        res = await obj.client._request(
            action="set_internal",
            path=("acl", name) + path,
            value="".join(v),
            iter=False,
            chain=res.get("chain", None),
        )

    if obj.meta:
        res = {
            "old": "".join(ov),
            "new": "".join(v),
            "chain": res.chain,
            "tock": res.tock,
        }
        yprint(res, stream=obj.stdout)
    else:
        res = {"old": "".join(ov), "new": "".join(v)}
        yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-m", "--mode", default=None, help="Mode letter to test.")
@click.option("-a", "--acl", default=None, help="ACL to test. Default: current")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def test(obj, name, path, acl, mode):
    """Test which ACL entry matches a path"""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if mode is not None and len(mode) != 1:
        raise click.UsageError("Mode must be one letter.")
    res = await obj.client._request(
        action="test_acl",
        path=path,
        iter=False,
        nchain=obj.meta,
        **({} if mode is None else {"mode": mode}),
        **({} if acl is None else {"acl": acl}),
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif isinstance(res.access, bool):
        print("+" if res.access else "-", file=obj.stdout)
    else:
        print(res.access, file=obj.stdout)
