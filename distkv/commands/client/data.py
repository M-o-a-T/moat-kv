# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

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



@main.group(short_help="Manage data.")
@click.pass_obj
async def cli(obj):
    """Manage users."""
    pass

@cli.command()
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


@cli.command()
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


@cli.command()
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


@cli.command()
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


@cli.command()
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


