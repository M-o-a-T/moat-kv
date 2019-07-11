# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

from range_set import RangeSet
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



@main.group(short_help="Control internal state.")
@click.pass_obj
async def cli(obj):
    """
    This subcommand queries and controls the server's internal state.
    """
    pass

@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-n", "--nodes", is_flag=True, help="Get node status.")
@click.option("-d", "--deleted", is_flag=True, help="Get deletion status.")
@click.option("-m", "--missing", is_flag=True, help="Get missing-node status.")
@click.option("-r", "--remote-missing", "remote_missing", is_flag=True, help="Get remote-missing-node status.")
@click.option("-k", "--known", is_flag=True, help="Get known-data status.")
@click.pass_obj
async def state(obj, yaml, **flags):
    """
    Dump the server's state.
    """
    if yaml:
        import yaml

    res = await obj.client._request("get_state", iter=False, **flags)
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False))
    else:
        pprint(res)


@cli.command()
@click.option("-d", "--deleted", is_flag=True, help="Mark as deleted. Default: known")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option("-n", "--node", "source", default='?', help="The node this message is faked as being from.")
@click.option("-b", "-broadcast", is_flag=True, help="Send to all servers")
@click.argument("node", nargs=1)
@click.argument("items", type=int, nargs=-1)
@click.pass_obj
async def mark(obj, deleted, source, node, items, yaml, broadcast):
    """
    Fix internal state.

    This is a dangerous command.
    """

    r = RangeSet()
    for i in items:
        r.add(i)
    k = "deleted" if deleted else "known"
    msg = {k: {node: r.__getstate__()}, "node":source}

    await obj.client._request("fake_info_send" if broadcast else "fake_info", iter=False, **msg)

    res = await obj.client._request("get_state", iter=False, **{k: True})
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False))
    else:
        pprint(res)



@cli.command()
@click.option("-d", "--delete", is_flag=True, help="Remove these nodes")
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.argument("nodes", nargs=-1)
@click.pass_obj
async def deleter(obj, delete, nodes, yaml):
    """
    Manage the Deleter list, which is the set of nodes that must be online
    for entry removal to happen.

    There should be one such node in every possible network partition.
    Also, all nodes with permanent storage should be on the list.
    """

    res = await obj.client._request(
        action="get_internal",
        path=("del",),
        iter=False,
        nchain=3 if delete or nodes else 2,
    )
    val = set(res.get('value', []))
    if delete:
        val -= set(nodes)
    elif nodes:
        val |= set(nodes)
    else:
        if yaml:
            import yaml
            print(yaml.safe_dump(res, default_flow_style=False))
        else:
            pprint(res)
        return

    val = list(val)
    res = await obj.client._request(
        action="set_internal",
        path=("del",),
        iter=False,
        chain=res.chain,
        value=val
    )
    res.value = val
    if yaml:
        import yaml
        print(yaml.safe_dump(res, default_flow_style=False))
    else:
        pprint(res)

