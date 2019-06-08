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



@main.group(short_help="Manage data.")
@click.pass_obj
async def cli(obj):
    """
    This subcommand accesses the actual user data stored in your DistKV tree.
    """
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
    """
    Read a DistKV value.

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """
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
        try:
            res = res.value
        except AttributeError:
            if obj.debug:
                print("No data at", repr(path), file=sys.stderr)
            sys.exit(1)
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False))
    else:
        pprint(res)


@cli.command(short_help="Add or update an entry")
@click.option("-v", "--value", help="The value to store. Mandatory.")
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
@click.option("-n", "--new", is_flag=True, help="This is a new entry.")
@click.option(
    "-y", "--yaml", is_flag=True, help="Print result as YAML. Default: Python."
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, value, eval, chain, prev, last, new, yaml):
    """
    Store a value at some DistKV position.

    If you update a value, you really should use "--last" (preferred) or
    "--prev" (if you must) to ensure that no other change arrived.

    When adding a new entry, use "--new" to ensure that you don't 
    accidentally overwrite something.
    """
    if eval:
        value = __builtins__["eval"](value)
    args = {}
    if new:
        if prev or last:
            raise click.UsageError("'new' and 'prev'/'last' are mutually exclusive")
        args["chain"] = None
    else:
        if prev is not NotGiven:
            if eval:
                prev = __builtins__["eval"](prev)
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client.set(*path, value=value, nchain=chain, **args)
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False))
    elif chain:
        pprint(res)


@cli.command(short_help="Delete an entry / subtree")
@click.argument("path", nargs=-1)
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
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.option("-e", "--eval", is_flag=True, help="The previous value shall be evaluated.")
@click.pass_obj
async def delete(obj, path, chain, prev, last, recursive, eval):
    """
    Delete an entry, or a whole subtree.

    You really should use "--last" (preferred) or
    "--prev" (if you must) to ensure that no other change arrived (but note
    that this doesn't work when deleting a subtree).

    Non-recursively deleting an entry with children works and does *not*
    affect the child entries.

    The root entry cannot be deleted.
    """
    args = {}
    if eval and not prev:
        raise click.UsageError("You need to add a value that can be evaluated")
    if recursive:
        if prev or last:
            raise click.UsageError("'local' and 'force' are mutually exclusive")
    else:
        if prev is not NotGiven:
            if eval:
                prev = __builtins__["eval"](prev)
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client.set(*path, value=value, nchain=chain, **args)

    res = await obj.client._request(
        action="delete_tree" if recursive else "delete_value", path=path, nchain=chain, **args
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
    flushing = not state
    async with obj.client.watch(*path, nchain=chain, fetch=state) as res:
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            if not flushing and r.get('state','') == "uptodate":
                flushing = True
            del r["seq"]
            if yaml:
                print(yaml.safe_dump(r, default_flow_style=False))
            else:
                pprint(r)
            if flushing:
                sys.stdout.flush()


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


