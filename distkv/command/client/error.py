# command line interface

import asyncclick as click
import datetime

from distkv.errors import ErrorRoot
from distkv.util import yprint, P

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
@click.pass_obj
async def cli(obj):
    """Manage error records in DistKV."""
    obj.err = await ErrorRoot.as_handler(obj.client)


@cli.command()
@click.option("-n", "--node", help="add details from this node")
@click.option(
    "-s", "--subsystem", "subsys", help="only show errors from this subsystem"
)
@click.option("-r", "--resolved", is_flag=True, help="only resolved errors")
@click.option("-t", "--trace", is_flag=True, help="add traces, if present")
@click.option("-a", "--all-nodes", is_flag=True, help="add details from all nodes")
@click.option(
    "-d", "--as-dict", default=None, help="Dump a list of all open (or resolved) error."
)
@click.argument("path", nargs=1)
@click.pass_obj
async def dump(obj, as_dict, path, node, all_nodes, trace, resolved, subsys):
    """Dump error entries.
    """
    path=P(path)
    path_ = obj.cfg["errors"].prefix
    d = 1
    if node is not None:
        path_ += (node,)
    else:
        d += 1

    async def one(r):
        nonlocal y
        val = r.value
        if "resolved" not in val and not all_nodes:
            return
        if resolved == (not val.get("resolved", False)):
            return
        fs = val.get('first_seen')
        if fs:
            val.first_seen_date = datetime.datetime.fromtimestamp(fs).strftime("%Y-%m-%d %H:%M:%S")
        ls = val.get('last_seen')
        if ls:
            val.last_seen_date = datetime.datetime.fromtimestamp(ls).strftime("%Y-%m-%d %H:%M:%S")
        fs = val.get('seen')
        if fs:
            val['seen_date'] = datetime.datetime.fromtimestamp(fs).strftime("%Y-%m-%d %H:%M:%S")
        try:
            rp = val.path
            if as_dict:
                del val.path
        except AttributeError:
            rp = ("incomplete",) + r.path
            if not as_dict:
                val.path = rp
        if rp[: len(path)] != path:
            return
        rp = rp[len(path) :]
        if node is None:
            val.syspath = r.path
        else:
            val.syspath = (node,) + r.path

        rn = {}
        if all_nodes or trace:
            rs = await obj.client._request(
                action="get_tree",
                min_depth=1,
                max_depth=1,
                path=path_ + r.path[-2:],
                iter=True,
                nchain=3 if obj.meta else 0,
            )
            async for rr in rs:
                val = rr.value
                fs = val.get('seen')
                if fs:
                    val['seen_date'] = datetime.datetime.fromtimestamp(fs).strftime("%Y-%m-%d %H:%M:%S")
                if not all_nodes:
                    try:
                        rn[rr.path[-1]] = rr.value.trace
                    except AttributeError:
                        continue
                else:
                    if not trace:
                        rr.value.pop("trace", None)
                    rn[rr.path[-1]] = rr if obj.meta else rr.value

            if rn:
                val["nodes"] = rn

        if as_dict is not None:
            yy = y
            if subsys is None:
                yy = yy.setdefault(val.get("subsystem", "unknown"), {})
            for p in rp:
                yy = yy.setdefault(p, {})
            yy[as_dict] = r if obj.meta else val
        else:
            yy = r if obj.meta else r.value

            yprint([yy], stream=obj.stdout)
            print("---", file=obj.stdout)

    y = {}
    res = None

    if node is not None and len(path) == 1:  # single error?
        try:
            if len(path) != 2:
                raise ValueError
            tock = int(path[1])
        except ValueError:
            pass
        else:
            r = await obj.client.get(path_, tock, nchain=3 if obj.meta else 0)
            # Mangle a few variables so that the output is still OK
            path = ()
            path_ = path_[:-1]
            r.path = (node, tock)
            node = None

            async def ait(r):
                yield r

            res = ait(r)

    if res is None:
        res = obj.client.get_tree(
            path_, min_depth=d, max_depth=d, nchain=3 if obj.meta else 0
        )
    async for r in res:
        await one(r)

    if as_dict is not None:
        yprint(y, stream=obj.stdout)
