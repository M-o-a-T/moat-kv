# command line interface

import os
import sys
import time
import asyncclick as click

from distkv.util import (
    PathLongener,
    MsgReader,
    PathShortener,
    NotGiven,
    yprint,
    data_get,
    path_eval,
)
from distkv.client import StreamedRequest

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage data.")  # pylint: disable=undefined-variable
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
@click.option("-V", "--eval-path", type=int,multiple=True, help="Eval this path element")
@click.option(
    "-R", "--raw", is_flag=True, help="Print string values without quotes etc."
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, **k):
    """
    Read a DistKV value.

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    await data_get(obj, *path, **k)


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
async def list(obj, path, **k):
    """
    List DistKV values.

    This is like "get" but with "--mindepth=1 --maxdepth=1 --recursive --empty"

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    k["recursive"] = True
    k["raw"] = True
    k["empty"] = True
    await data_get(obj, *path, **k)


@cli.command(short_help="Add or update an entry")
@click.option("-v", "--value", help="The value to store. Mandatory.")
@click.option("-e", "--eval", "eval_", is_flag=True, help="The value shall be evaluated.")
@click.option(
    "-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-n", "--new", is_flag=True, help="This is a new entry.")
@click.option("-V", "--eval-path", type=int,multiple=True, help="Eval this path element")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, eval_path, value, eval_, prev, last, new):
    """
    Store a value at some DistKV position.

    If you update a value, you really should use "--last" (preferred) or
    "--prev" (if you must) to ensure that no other change arrived.

    When adding a new entry, use "--new" to ensure that you don't 
    accidentally overwrite something.
    """
    if eval_:
        value = eval(value)
    args = {}
    if new:
        if prev is not NotGiven or last:
            raise click.UsageError("'new' and 'prev'/'last' are mutually exclusive")
        args["chain"] = None
    else:
        if prev is not NotGiven:
            if eval_:
                prev = eval(prev)
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client.set(*path_eval(path, eval_path), value=value, nchain=obj.meta, **args)
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command(short_help="Delete an entry / subtree")
@click.argument("path", nargs=-1)
@click.option(
    "-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.option("--internal", is_flag=True, help="Affect the internal tree. DANGER.")
@click.option("-V", "--eval-path", type=int,multiple=True, help="Eval this path element")
@click.option(
    "-e", "--eval", is_flag=True, help="The previous value shall be evaluated."
)
@click.pass_obj
async def delete(obj, path, eval_path, prev, last, recursive, eval, internal):
    """
    Delete an entry, or a whole subtree.

    You really should use "--last" (preferred) or "--prev" (if you must) to
    ensure that no other change arrived (but note that this doesn't work
    when deleting a subtree).

    Non-recursively deleting an entry with children works and does *not*
    affect the child entries.

    The root entry cannot be deleted.
    """
    args = {}
    if eval and prev is NotGiven:
        raise click.UsageError("You need to add a value that can be evaluated")
    if recursive:
        if prev is not NotGiven or last:
            raise click.UsageError("'local' and 'force' are mutually exclusive")
        if internal:
            raise click.UsageError("'internal' and 'recursive' are mutually exclusive")
    else:
        if prev is not NotGiven:
            if eval:
                prev = __builtins__["eval"](prev)
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client._request(
        action="delete_tree" if recursive else "delete_internal" if internal else "delete_value",
        path=tuple(path_eval(path,eval_path)),
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
@click.option("-V", "--eval-path", type=int,multiple=True, help="Eval this path element")
@click.argument("path", nargs=-1)
@click.pass_obj
async def watch(obj, path, eval_path, state):
    """Watch a DistKV subtree"""
    flushing = not state
    path = tuple(path_eval(path, eval_path))

    async with obj.client.watch(*path, nchain=obj.meta, fetch=state) as res:
        pl = PathLongener(path)
        async for r in res:
            pl(r)
            if not flushing and r.get("state", "") == "uptodate":
                flushing = True
            del r["seq"]
            r['time'] = time.monotonic()
            yprint(r, stream=obj.stdout)
            print("---", file=obj.stdout)
            if flushing:
                obj.stdout.flush()


@cli.command()
@click.option("-l", "--local", is_flag=True, help="Load locally, don't broadcast")
@click.option("-f", "--force", is_flag=True, help="Overwrite existing values")
@click.option("-i", "--infile", type=click.File("rb"), help="File to read (msgpack).")
@click.option("-V", "--eval-path", type=int,multiple=True, help="Eval this path element")
@click.argument("path", nargs=-1)
@click.pass_obj
async def update(obj, path, eval_path, infile, local, force):
    """Send a list of updates to a DistKV subtree"""
    if local and force:
        raise click.UsageError("'local' and 'force' are mutually exclusive")

    path = tuple(path_eval(path, eval_path))
    ps = PathShortener(path)
    async with MsgReader(path=path) as reader:
        with obj.client._stream(
            action="update", path=path, force=force, local=local
        ) as sender:
            async for r in reader:
                ps(r)
                await sender.send(r)

    print(sender.result)
