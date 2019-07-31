# command line interface

import os
import sys
import asyncclick as click

from distkv.util import (
    attrdict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.client import StreamedRequest
from distkv.command import Loader
from distkv.default import CFG
from distkv.server import Server
from distkv.auth import loader, gen_auth
from distkv.exceptions import ClientError
from distkv.util import yprint

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
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
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
@click.option("-e", "--empty", is_flag=True, help="Include empty nodes")
@click.option(
    "-R", "--raw", is_flag=True, help="Print string values without quotes etc."
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(*a, **k):
    """
    Read a DistKV value.

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    await _get(*a, **k)


@cli.command()
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.option(
    "-m",
    "--maxdepth",
    type=int,
    default=1,
    help="Limit recursion depth. Default: 1 (single layer).",
)
@click.option(
    "-M",
    "--mindepth",
    type=int,
    default=1,
    help="Starting depth. Default: 1 (single layer).",
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def list(*a, **k):
    """
    List DistKV values.

    This is like "get" but with "--mindepth=1 --maxdepth=1 --recursive"

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    k["recursive"] = True
    k["raw"] = False
    k["empty"] = False
    await _get(*a, **k)


async def _get(obj, path, recursive, as_dict, maxdepth, mindepth, empty, raw):
    if recursive:
        if raw:
            raise click.UsageError("'raw' cannot be used with 'recursive'")

        kw = {}
        if maxdepth is not None:
            kw["max_depth"] = maxdepth
        if mindepth is not None:
            kw["min_depth"] = mindepth
        if empty:
            kw["add_empty"] = True
        y = {}
        async for r in obj.client.get_tree(*path, nchain=obj.meta, **kw):
            r.pop("seq", None)
            path = r.pop("path")
            if as_dict is not None:
                yy = y
                for p in path:
                    yy = yy.setdefault(p, {})
                try:
                    yy[as_dict] = r if obj.meta else r.value
                except AttributeError:
                    continue
            else:
                y = {}
                try:
                    y[path] = r if obj.meta else r.value
                except AttributeError:
                    continue
                yprint([y], stream=obj.stdout)

        if as_dict is not None:
            yprint(y, stream=obj.stdout)
        return

    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    if as_dict is not None:
        raise click.UsageError("'as-dict' only works with 'recursive'")
    res = await obj.client.get(*path, nchain=obj.meta)
    if not obj.meta:
        try:
            res = res.value
        except AttributeError:
            if obj.debug:
                print("No data at", repr(path), file=sys.stderr)
            sys.exit(1)

    if not raw:
        yprint(res, stream=obj.stdout)
    elif isinstance(res, bytes):
        os.write(obj.stdout.fileno(), res)
    else:
        obj.stdout.write(str(res))


@cli.command(short_help="Add or update an entry")
@click.option("-v", "--value", help="The value to store. Mandatory.")
@click.option("-e", "--eval", is_flag=True, help="The value shall be evaluated.")
@click.option(
    "-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-n", "--new", is_flag=True, help="This is a new entry.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, value, eval, prev, last, new):
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

    res = await obj.client.set(*path, value=value, nchain=obj.meta, **args)
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command(short_help="Delete an entry / subtree")
@click.argument("path", nargs=-1)
@click.option(
    "-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.option(
    "-e", "--eval", is_flag=True, help="The previous value shall be evaluated."
)
@click.pass_obj
async def delete(obj, path, prev, last, recursive, eval):
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

    res = await obj.client._request(
        action="delete_tree" if recursive else "delete_value",
        path=path,
        nchain=obj.meta,
        **args
    )
    if isinstance(res, StreamedRequest):
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            if obj.meta:
                yprint(r, stream=obj.stdout)
    else:
        if obj.meta:
            yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-s", "--state", is_flag=True, help="Also get the current state.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def watch(obj, path, state):
    """Watch a DistKV subtree"""
    flushing = not state
    async with obj.client.watch(*path, nchain=obj.meta, fetch=state) as res:
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            if not flushing and r.get("state", "") == "uptodate":
                flushing = True
            del r["seq"]
            yprint(r, stream=obj.stdout)
            if flushing:
                obj.stdout.flush()


@cli.command()
@click.option("-l", "--local", is_flag=True, help="Load locally, don't broadcast")
@click.option("-f", "--force", is_flag=True, help="Overwrite existing values")
@click.option("-i", "--infile", type=click.File("rb"), help="File to read (msgpack).")
@click.argument("path", nargs=-1)
@click.pass_obj
async def update(obj, path, infile, local, force):
    """Send a list of updates to a DistKV subtree"""
    if local and force:
        raise click.UsageError("'local' and 'force' are mutually exclusive")

    ps = PathShortener(path)
    async with MsgReader(path=path) as reader:
        with obj.client._stream(
            action="update", path=path, force=force, local=local
        ) as sender:
            async for r in reader:
                ps(r)
                await sender.send(r)

    print(sender.result)
