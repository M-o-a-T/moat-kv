# command line interface

import time
import asyncclick as click
import datetime

from distkv.util import PathLongener, MsgReader, NotGiven, yprint, P, attr_args
from distkv.client import StreamedRequest
from distkv.data import data_get, node_attr, add_dates


@click.group(
    short_help="Manage data.", invoke_without_command=True
)  # pylint: disable=undefined-variable
@click.argument("path", type=P, nargs=1)
@click.pass_context
async def cli(ctx, path):
    """
    This subcommand accesses the actual user data stored in your DistKV tree.
    """
    if ctx.invoked_subcommand is None:
        await data_get(ctx.obj, path, recursive=False)
    else:
        ctx.obj.path = path


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
@click.option("-D", "--add-date", is_flag=True, help="Add *_date entries")
@click.pass_obj
async def get(obj, **k):
    """
    Read a DistKV value.

    If you read a sub-tree recursively, be aware that the whole subtree
    will be read before anything is printed. Use the "watch --state" subcommand
    for incremental output.
    """

    await data_get(obj, obj.path, **k)


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
@click.pass_obj
async def list_(obj, **k):
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
    await data_get(obj, obj.path, **k)


@cli.command("set", short_help="Add or update an entry")
@attr_args
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-n", "--new", is_flag=True, help="This is a new entry.")
@click.pass_obj
async def set_(obj, vars_, eval_, path_, last, new):
    """
    Store a value at some DistKV position.

    If you update a value you can use "--last" to ensure that no other
    change arrived between reading and writing the entry. (This is not
    foolproof but reduces the window to a fraction of a second.)

    When adding a new entry use "--new" to ensure that you don't
    accidentally overwrite something.

    DistKV entries typically are mappings. Use a colon as the path if you
    want to replace the top level.
    """
    args = {}
    if new:
        if last:
            raise click.UsageError("'new' and 'last' are mutually exclusive")
        args["chain"] = None
    else:
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await node_attr(obj, obj.path, vars_, eval_, path_, **args)

    if obj.meta:
        yprint(res, stream=obj.stdout)


class nstr:
    def __new__(cls, val):
        if val is NotGiven:
            return val
        return str(val)


@cli.command(short_help="Delete an entry / subtree")
@click.option(
    "-p", "--prev", type=nstr, default=NotGiven, help="Previous value. Deprecated; use 'last'"
)
@click.option("-l", "--last", nargs=2, help="Previous change entry (node serial)")
@click.option("-r", "--recursive", is_flag=True, help="Delete a complete subtree")
@click.option("--internal", is_flag=True, help="Affect the internal tree. DANGER.")
@click.option("-e", "--eval", "eval_", is_flag=True, help="The previous value shall be evaluated.")
@click.pass_obj
async def delete(obj, prev, last, recursive, eval_, internal):
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
    if eval_ and prev is NotGiven:
        raise click.UsageError("You need to add a value that can be evaluated")
    if recursive:
        if prev is not NotGiven or last:
            raise click.UsageError("You can't use a prev value when deleting recursively.")
        if internal:
            raise click.UsageError("'internal' and 'recursive' are mutually exclusive")
    else:
        if prev is not NotGiven:
            if eval_:
                prev = eval(prev)  # pylint: disable=eval-used
            args["prev"] = prev
        if last:
            args["chain"] = {"node": last[0], "tick": int(last[1])}

    res = await obj.client.delete(path=obj.path, nchain=obj.meta, recursive=recursive, **args)
    if isinstance(res, StreamedRequest):
        pl = PathLongener(obj.path)
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
@click.option("-D", "--add-date", is_flag=True, help="Add *_date entries")
@click.option("-i", "--ignore", multiple=True, type=P, help="Skip this (sub)tree")
@click.pass_obj
async def monitor(obj, state, only, add_date, ignore):
    """Monitor a DistKV subtree"""

    flushing = not state
    seen = False

    async with obj.client.watch(
        obj.path, nchain=obj.meta, fetch=state, max_depth=0 if only else -1
    ) as res:
        async for r in res:
            if add_date and "value" in r:
                add_dates(r.value)
            if any(p == r.path[: len(p)] for p in ignore):
                continue
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
                        continue
            if flushing:
                r["time"] = time.time()
                r["_time"] = datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
            yprint(r, stream=obj.stdout)
            print("---", file=obj.stdout)
            if flushing:
                obj.stdout.flush()
            seen = True


@cli.command()
@click.option("-i", "--infile", type=click.Path(), help="File to read (msgpack).")
@click.pass_obj
async def update(obj, infile):
    """Send a list of updates to a DistKV subtree"""
    async with MsgReader(path=infile) as reader:
        async for msg in reader:
            if not hasattr(msg, "path"):
                continue
            await obj.client.set(obj.path + msg.path, value=msg.value)
