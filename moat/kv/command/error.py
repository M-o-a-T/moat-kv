# command line interface

import sys

import asyncclick as click
from moat.util import P, Path, yprint

from moat.kv.data import add_dates
from moat.kv.errors import ErrorRoot


@click.group()  # pylint: disable=undefined-variable
@click.pass_obj
async def cli(obj):
    """Manage error records in MoaT-KV."""
    obj.err = await ErrorRoot.as_handler(obj.client)


@cli.command()
@click.option("-s", "--subsys", help="Subsystem to access")
@click.argument("path", nargs=1)
@click.pass_obj
async def resolve(obj, path, subsys):
    """
    Mark an error as resolved.
    """
    path = P(path)
    if subsys:
        e = obj.err.get_error_record(subsys, path)
    else:
        e = obj.err.follow(path)
    if e.resolved:
        print("Already resolved.", file=sys.stderr)
        return
    await e.resolve()


@cli.command()
@click.option("-n", "--node", help="add details from this node")
@click.option(
    "-s", "--subsystem", "subsys", help="only show errors from this subsystem"
)
@click.option("-r", "--resolved", is_flag=True, help="only resolved errors")
@click.option(
    "-v", "--verbose", count=True, help="add per-node details (-vv for traces)"
)
@click.option("-a", "--all-errors", is_flag=True, help="add details from all nodes")
@click.option(
    "-d", "--as-dict", default=None, help="Dump a list of all open (or resolved) error."
)
@click.option("-p", "--path", default=":", help="only show errors below this subpath")
@click.pass_obj
async def dump(obj, as_dict, path, node, all_errors, verbose, resolved, subsys):
    """Dump error entries."""
    path = P(path)
    path_ = obj.cfg["errors"].prefix
    d = 2

    async def one(r):
        nonlocal y
        val = r.value
        if subsys and val.get("subsystem", "") != subsys:
            return
        if path and val.get("path", ())[: len(path)] != path:
            return

        if not all_errors and bool(val.get("resolved", False)) != resolved:
            return

        add_dates(val)
        try:
            rp = val.path
            if as_dict:
                del val.path
            elif not isinstance(rp, Path):
                rp = Path.build(rp)
        except AttributeError:
            rp = Path("incomplete") + r.path
            if not as_dict:
                val.path = rp
        if rp[: len(path)] != path:
            return
        rp = rp[len(path) :]
        if node is None:
            val.syspath = r.path
        else:
            val.syspath = Path(node) + r.path

        rn = {}

        def disp(rr):
            if node and rr.path[-1] != node:
                return
            val = rr.value
            add_dates(val)
            if verbose < 2:
                rr.value.pop("trace", None)
            rn[rr.path[-1]] = rr if obj.meta else rr.value

        if verbose:
            rs = await obj.client._request(
                action="get_tree",
                min_depth=1,
                max_depth=1,
                path=path_ + r.path[-2:],
                iter=True,
                nchain=3 if obj.meta else 0,
            )
            async for rr in rs:
                disp(rr)

        elif node:
            rs = await obj.client.get(path_ + r.path[-2:] | node)
            if "value" in rs:
                disp(rs)

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

    if node is None and len(path) == 2 and isinstance(path[-1], int):  # single error?
        r = await obj.client.get(path_ + path, nchain=3 if obj.meta else 0)
        # Mangle a few variables so that the output is still OK
        r.path = path
        node = None

        async def ait(r):
            yield r

        res = ait(r)
        path = ()

    if res is None:
        res = obj.client.get_tree(
            path_, min_depth=d, max_depth=d, nchain=3 if obj.meta else 0
        )
    async for r in res:
        await one(r)

    if as_dict is not None:
        yprint(y, stream=obj.stdout)
