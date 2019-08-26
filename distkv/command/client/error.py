# command line interface

import asyncclick as click
import time

from distkv.exceptions import ServerError
from distkv.errors import ErrorRoot
from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
@click.pass_obj
async def cli(obj):
    """Manage error records in DistKV."""
    obj.err = await ErrorRoot.as_handler(obj.client)


@cli.command()
@click.option("-n", "--node", help="add details from this node")
@click.option("-t", "--trace", is_flag=True, help="add traces, if present")
@click.option("-a", "--all-nodes", is_flag=True, help="add details from all nodes")
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="YAML: structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def dump(obj, as_dict, path, node, all_nodes, trace):
    """Dump error entries.
    """
    path_ = obj.cfg["errors"].prefix
    if node:
        res = obj.client.get_tree(
            *path_, node, min_depth=1, max_depth=1, nchain=3 if obj.meta else 0
        )
    else:
        res = obj.client.get_tree(
            *path_, min_depth=2, max_depth=2, nchain=3 if obj.meta else 0
        )
    # TODO increase max_depth and attach the data to

    y = {}
    async for r in res:
        try:
            rp = r.value.path
            if rp[: len(path)] != path:
                continue
        except AttributeError:
            r['state']='incomplete'
            yprint(r, stream=obj.stdout)
            print("---", file=obj.stdout)
            continue
        rp = rp[len(path) :]
        rpe = r.pop("path")
        r.value.path = rpe[-2:]

        if all_nodes:
            rn = {}
            rs = await obj.client._request(
                action="get_tree",
                min_depth=1,
                max_depth=1,
                path=rpe,
                iter=True,
                nchain=3 if obj.meta else 0,
            )
            async for rr in rs:
                if not trace:
                    rr.value.pop('trace', None)
                rn[rr.path[-1]] = rr if obj.meta else rr.value
        elif node is not None:
            rn = {}
            rr = await obj.client._request(
                action="get_value",
                path=rpe + (node,),
                iter=False,
                nchain=3 if obj.meta else 0,
            )
            if "value" not in rr:
                continue
            if rr is not None and "value" in rr:
                rr.value.pop('trace', None)
                rn[node] = rr if obj.meta else rr.value
        else:
            rn = None

        if as_dict is not None:
            yy = y
            for p in rp:
                yy = yy.setdefault(p, {})
            yy[as_dict] = r if obj.meta else r.pop("value")
        else:
            yy = {}
            yy[rp] = r if obj.meta else r.value

        if rn:
            yy["nodes"] = rn

        if as_dict is None:
            yprint([yy], stream=obj.stdout)
            print("---", file=obj.stdout)

    if as_dict is not None:
        yprint(y, stream=obj.stdout)


