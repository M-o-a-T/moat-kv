# command line interface

import asyncclick as click

from range_set import RangeSet
from distkv.util import yprint, PathLongener
from collections.abc import Mapping

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Control internal state.")  # pylint: disable=undefined-variable
@click.pass_obj
async def cli(obj):
    """
    This subcommand queries and controls the server's internal state.
    """
    pass


@cli.command()
@click.option("-n", "--nodes", is_flag=True, help="Get node status.")
@click.option("-d", "--deleted", is_flag=True, help="Get deletion status.")
@click.option("-m", "--missing", is_flag=True, help="Get missing-node status.")
@click.option(
    "-r",
    "--remote-missing",
    "remote_missing",
    is_flag=True,
    help="Get remote-missing-node status.",
)
@click.option("-p", "--present", is_flag=True, help="Get known-data status.")
@click.option("-s", "--superseded", is_flag=True, help="Get superseded-data status.")
@click.option("-k", "--known", hidden=True, is_flag=True, help="Get superseded-data status.")
@click.option("-a", "--all", is_flag=True, help="All available data.")
@click.pass_obj
async def state(obj, **flags):
    """
    Dump the server's state.
    """
    if flags.pop('known',None):
        flags['superseded'] = True
    if flags.pop('all',None):
        flags['superseded'] = True
        flags['present'] = True
        flags['nodes'] = True
        flags['deleted'] = True
        flags['missing'] = True
        flags['remote_missing'] = True
    res = await obj.client._request("get_state", iter=False, **flags)
    k = res.pop('known', None)
    if k is not None:
        res['superseded'] = k
    yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-d", "--deleted", is_flag=True, help="Mark as deleted. Default: superseded")
@click.option(
    "-n",
    "--node",
    "source",
    default="?",
    help="The node this message is faked as being from.",
)
@click.option("-b", "--broadcast", is_flag=True, help="Send to all servers")
@click.argument("node", nargs=1)
@click.argument("items", type=int, nargs=-1)
@click.pass_obj
async def mark(obj, deleted, source, node, items, broadcast):
    """
    Fix internal state. Use no items to fetch the current list from the
    server's ``missing`` state. Use an empty node name to add the whole
    list, not just a single node's.

    This is a dangerous command.
    """

    k = "deleted" if deleted else "superseded"
    if not items:
        r = await obj.client._request("get_state", iter=False, missing=True)
        r = r["missing"]
        if node != "":
            r = {node: r[node]}
    elif node == "":
        raise click.UsageError("You can't do that with an empty node")
    else:
        r = RangeSet()
        for i in items:
            r.add(i)
        r = {node: r.__getstate__()}

    msg = {k: r, "node": source}

    await obj.client._request("fake_info", iter=False, **msg)
    if broadcast:
        await obj.client._request("fake_info_send", iter=False, **msg)

    res = await obj.client._request("get_state", iter=False, **{k: True})
    yprint(res, stream=obj.stdout)


@cli.command(short_help="Manage the Deleter list")
@click.option("-d", "--delete", is_flag=True, help="Remove these nodes")
@click.argument("nodes", nargs=-1)
@click.pass_obj
async def deleter(obj, delete, nodes):
    """
    Manage the Deleter list

    This is the set of nodes that must be online for removal of deleted
    entries from DistKV's data.

    There should be one such node in every possible network partition.
    Also, all nodes with permanent storage should be on the list.

    Usage:
    - … deleter          -- list state
    - … deleter NODE…    -- add this node
    - … deleter -d NODE… -- remove this node
    """

    res = await obj.client._request(
        action="get_internal",
        path=("actor","del",),
        iter=False,
        nchain=3 if delete or nodes else 2,
    )
    val = res.get("value", {})
    if isinstance(val, Mapping):
        val = val.get("nodes", [])
    # else: compatibility, TODO remove
    val = set(val)
    if delete:
        if nodes:
            val -= set(nodes)
        else:
            val = set()
    elif nodes:
        val |= set(nodes)
    else:
        yprint(res, stream=obj.stdout)
        return

    val = {'nodes': list(val)}
    res = await obj.client._request(
        action="set_internal", path=("actor","del",), iter=False, chain=res.chain, value=val
    )
    res.value = val
    yprint(res, stream=obj.stdout)


@cli.command()
@click.argument("path", nargs=-1)
@click.pass_obj
async def dump(obj, path):
    """
    Dump internal state.

    This displays DistKV's internal state.
    """

    y = {}
    pl=PathLongener()
    async for r in await obj.client._request(
        "get_tree_internal", path=path, iter=True, nchain=0
    ):
        pl(r)
        path = r["path"]
        yy = y
        for p in path:
            yy = yy.setdefault(p, {})
        try:
            yy["_"] = r["value"]
        except KeyError:
            pass
    yprint(y, stream=obj.stdout)
