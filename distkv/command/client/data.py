# command line interface

import time
import asyncclick as click
import datetime

from distkv.util import PathLongener, MsgReader, NotGiven, yprint, data_get, P
from distkv.client import StreamedRequest
from distkv.command import node_attr

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage data.")  # pylint: disable=undefined-variable
async def cli():
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
    "-m", "--maxdepth", type=int, default=None, help="Limit recursion depth. Default: whole tree"
)
@click.option(
    "-M", "--mindepth", type=int, default=None, help="Starting depth. Default: whole tree"
)
@click.option("-r", "--recursive", is_flag=True, help="Read a complete subtree")
@click.option("-e", "--empty", is_flag=True, help="Include empty nodes")
@click.option("-R", "--raw", is_flag=True, help="Print string values without quotes etc.")
@click.argument("path", nargs=1)
@click.pass_obj
async def get(obj, path, **k):
    """
    Read a DistKV value.

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    path = P(path)
    await data_get(obj, path, **k)


@cli.command("list")
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
    "-M", "--mindepth", type=int, default=1, help="Starting depth. Default: 1 (single layer)."
)
@click.argument("path", nargs=1)
@click.pass_obj
async def list_(obj, path, **k):
    """
    List DistKV values.

    This is like "get" but with "--mindepth=1 --maxdepth=1 --recursive --empty"

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    path = P(path)
    k["recursive"] = True
    k["raw"] = True
    k["empty"] = True
    await data_get(obj, path, **k)


@cli.command("set", short_help="Add or update an entry")
@click.option("-v", "--value", help="The value to store. Mandatory.")
@click.option("-e", "--eval", "eval_", is_flag=True, help="The value shall be evaluated.")
@click.option("-P", "--path", "path_", is_flag=True, help="The value is a path.")
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-n", "--new", is_flag=True, help="This is a new entry.")
@click.option("-a", "--attr", default=None, help="Modify attribute.")
@click.argument("path", nargs=1)
@click.pass_obj
async def set_(obj, path, value, eval_, last, new, path_, attr):
    """
    Store a value at some DistKV position.

    If you update a value, you should use "--last"
    to ensure that no other change arrived.

    When adding a new entry, use "--new" to ensure that you don't
    accidentally overwrite something.
    """
    path = P(path)
    if attr is not None:
        attr = P(attr)
    if eval_:
        if path_:
            raise click.UsageError("'eval' and 'path' are mutually exclusive")
        if value == "-":
            value = NotGiven
        else:
            value = eval(value)  # pylint: disable=eval-used
            eval_ = False
    elif path_:
        value = P(value)
    args = {}
    if new:
        if last:
            raise click.UsageError("'new' and 'last' are mutually exclusive")
        args["chain"] = None
    else:
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    if attr is not None:
        res = await node_attr(obj, path, attr, value, eval_=eval_, **args)
    else:
        res = await obj.client.set(path, value=value, nchain=obj.meta, **args)

    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command(short_help="Delete an entry / subtree")
@click.argument("path", nargs=1)
@click.option("-p", "--prev", default=NotGiven, help="Previous value. Deprecated; use 'last'")
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.option("--internal", is_flag=True, help="Affect the internal tree. DANGER.")
@click.option("-e", "--eval", "eval_", is_flag=True, help="The previous value shall be evaluated.")
@click.pass_obj
async def delete(obj, path, prev, last, recursive, eval_, internal):
    """
    Delete an entry, or a whole subtree.

    You really should use "--last" (preferred) or "--prev" (if you must) to
    ensure that no other change arrived (but note that this doesn't work
    when deleting a subtree).

    Non-recursively deleting an entry with children works and does *not*
    affect the child entries.

    The root entry cannot be deleted.
    """
    path = P(path)
    args = {}
    if eval_ and prev is NotGiven:
        raise click.UsageError("You need to add a value that can be evaluated")
    if recursive:
        if prev is not NotGiven or last:
            raise click.UsageError("'local' and 'force' are mutually exclusive")
        if internal:
            raise click.UsageError("'internal' and 'recursive' are mutually exclusive")
    else:
        if prev is not NotGiven:
            if eval_:
                prev = eval(prev)  # pylint: disable=eval-used
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client._request(
        action="delete_tree" if recursive else "delete_internal" if internal else "delete_value",
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
@click.option("-o", "--only", is_flag=True, help="Value only, nothing fancy.")
@click.argument("path", nargs=1)
@click.pass_obj
async def watch(obj, path, state, only):
    """Watch a DistKV subtree"""

    flushing = not state
    path = P(path)
    seen = False

    async with obj.client.watch(
        path, nchain=obj.meta, fetch=state, max_depth=0 if only else -1
    ) as res:
        async for r in res:
            if r.get("state", "") == "uptodate":
                if only and not seen:
                    # value doesn't exist
                    return
                flushing = True
            else:
                del r["seq"]
                if only:
                    try:
                        print(r.value, file=obj.stdout)
                        continue
                    except AttributeError:
                        # value has been deleted
                        return
            if flushing:
                r["time"] = time.monotonic()
                r["date"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yprint(r, stream=obj.stdout)
            print("---", file=obj.stdout)
            if flushing:
                obj.stdout.flush()
            seen = True


@cli.command()
@click.option("-i", "--infile", type=click.Path(), help="File to read (msgpack).")
@click.argument("path", nargs=1)
@click.pass_obj
async def update(obj, path, infile):
    """Send a list of updates to a DistKV subtree"""
    path = P(path)
    async with MsgReader(path=infile) as reader:
        async for msg in reader:
            await obj.client.set(*path, *msg.path, value=msg.value)
