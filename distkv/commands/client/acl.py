# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

from distkv.util import (
    attrdict,
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

ACL = set("rwdcxena")
# read, write, delete, create, access, enumerate

@main.group()
@click.pass_obj
async def cli(obj):
    """Manage ACLs. Usage: … acl …"""
    pass


@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.pass_obj
async def list(obj, yaml, verbose):
    """List ACLs.
    """
    res = await obj.client._request(
        action="enum_internal",
        path=("acl",),
        iter=False,
        nchain=3 if verbose else 0,
    )
    pprint(res)

@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-d", "--as-dict", is_flag=True, help="Dump as a dict. Default: List.")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def dump(obj, name, path):
    """Dump a complete (or partial) ACL."""
    res = await obj.client._request(
        action="get_tree_internal",
        path=("acl",name)+path,
        iter=True,
        nchain=3 if verbose else 0,
    )
    if yaml:
        import yaml
    y = {} if as_dict else []
    async for r in res:
        if yaml:
            if as_dict:
                yy = y
                for p in r.pop("path"):
                    yy = yy.setdefault(p, {})
                if "chain" in r:
                    yy["chain"] = r.chain
                yy[as_dict] = r.pop("value")
            else:
                y.append((res.path,res.value))

    if yaml:
        print(yaml.safe_dump(y, default_flow_style=False), file=sys.stdout)
    else:
        pprint(y)


@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, name, path, yaml, verbose):
    """Read an ACL.
    
    This command does not test a path. Use "… acl test …" for that.
    """
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal",
        path=("acl", name) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )

    if not verbose:
        res = res.value
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False), file=sys.stdout)
    else:
        pprint(res)


@cli.command(name="set")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-a", "--acl", help="The value to set. Start with '+' to add, '-' to remove rights.")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set_(obj, acl, name, path, verbose, yaml):
    """Set or change an ACL."""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if acl[0] in "+-":
        mode = acl[0]
        acl = acl[1:]
    else:
        mode = "r"
    acl = set(acl)

    if acl - ACL:
        raise click.UsageError("You're trying to set an unknown ACL flag: %r" % (acl-ACL,))

    res = await obj.client._request(
        action="get_internal",
        path=("acl", name) + path,
        iter=False,
        nchain=3 if verbose else 1,
    )
    ov = set(res.get('value', ''))
    if ov - ACL:
        print("Warning: original ACL contains unknown: %r" % (ov-acl,), file=sys.stderr)

    if mode == '-' and not acl:
        res = await obj.client._request(
            action="delete_internal",
            path=("acl", name) + path,
            iter=False,
            chain=res.chain,
        )
        v = "-"

    else:
        if mode == '+':
            v = ov+acl
        elif mode == '-':
            v = ov-acl
        else:
            v=acl
        res = await obj.client._request(
            action="set_internal",
            path=("acl", name) + path,
            value="".join(v),
            iter=False,
            chain=res.get('chain', None),
        )

    if verbose:
        res = {"old": "".join(ov), "new": "".join(v), "chain":res.chain, "tock":res.tock}
        if yaml:
            print(yaml.safe_dump(res, default_flow_style=False), file=sys.stdout)
        else:
            pprint(res)
    elif yaml:
        res = {"old": "".join(ov), "new": "".join(v)}
        print(yaml.safe_dump(res, default_flow_style=False), file=sys.stdout)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option('-m','--mode',default=None, help="Mode letter to test.")
@click.option('-a','--acl',default=None, help="ACL to test. Default: current")
@click.argument("path", nargs=-1)
@click.pass_obj
async def test(obj, path, acl, verbose, mode):
    """Test which ACL entry matches a path"""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if mode is not None and len(mode) != 1:
        raise click.UsageError("Mode must be one letter.")
    res = await obj.client._request(
        action="test_acl",
        path=path,
        iter=False,
        nchain=3 if verbose else 0,
        **({} if mode is None else {'mode': mode}),
        **({} if acl is None else {'acl': acl}),
    )
    if verbose:
        pprint(res)
    elif isinstance(res.access, bool):
        print('+' if res.access else '-')
    else:
        print(res.access)
