# command line interface

import sys
import asyncclick as click
import time
import anyio
import datetime

from distkv.exceptions import ServerError
from distkv.code import CodeRoot
from distkv.runner import AnyRunnerRoot, SingleRunnerRoot, AllRunnerRoot
from distkv.util import yprint, PathLongener, P, Path, data_get

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
@click.option("-n", "--node", help="node to run this code on. Empty: any one node, '-': all nodes")
@click.option("-g", "--group", help="group to run this code on. Empty: any one node")
@click.pass_obj
async def cli(obj, node, group):
    """Run code stored in DistKV."""
    if group is None:
        group = "default"
    if not node:
        obj.runner_root = AnyRunnerRoot
        subpath = (group,)
    elif node == "-":
        obj.runner_root = AllRunnerRoot
        subpath = (group,)
    else:
        obj.runner_root = SingleRunnerRoot

        subpath = (node, group)

    obj.subpath = Path(obj.cfg["runner"]["sub"][obj.runner_root.SUB]) + subpath
    obj.path = obj.cfg["runner"]["prefix"] + obj.subpath
    obj.statepath = obj.cfg["runner"]["state"] + obj.subpath


@cli.command("path")
@click.pass_obj
@click.argument("path", nargs=1)
async def path__(obj, path):
    """
    Emit the full path leading to the specified runner object.

    Useful for copying or for state monitoring.

    NEVER directly write to the state object. It's controlled by the
    runner. You'll confuse it if you do that.

    Updating the control object will cancel any running code.
    """
    path = P(path)
    res = dict(command=obj.subpath + path, path=obj.path, state=obj.statepath)
    yprint(res, stream=obj.stdout)


@cli.command("all")
@click.option(
    "-n", "--nodes", type=int, default=0, help="Size of the group (not for single-node runners)"
)
@click.pass_obj
async def all_(obj, nodes):
    """
    Run code that needs to run.

    This does not return.
    """
    from distkv.util import as_service

    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if nodes and obj.runner_root == SingleRunnerRoot:
        raise click.UsageError("A single-site runner doesn't have a size.")

    async with as_service(obj) as evt:
        c = obj.client
        cr = await CodeRoot.as_handler(c)
        await obj.runner_root.as_handler(
            c, subpath=obj.subpath, code=cr, **({"nodes": nodes} if nodes else {})
        )
        await evt.set()
        while True:
            await anyio.sleep(99999)


@cli.command("list")
@click.option("-s", "--state", is_flag=True, help="Add state data")
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.argument("path", nargs=1)
@click.pass_obj
async def list_(obj, state, as_dict, path):
    """List run entries.
    """
    path = P(path)
    if obj.subpath[-1] == "-":
        if path:
            raise click.UsageError("Group '-' can only be used without a path.")

        path = obj.path[:-1]
        res = await obj.client._request(
            action="get_tree", path=path, iter=True, max_depth=2, nchain=obj.meta, empty=True
        )
        pl = PathLongener(())
        async for r in res:
            pl(r)
            print(r.path[-1], file=obj.stdout)
        return

    if state:
        state = obj.statepath + path
    path = obj.path + path

    async def state_getter(r):
        val = r.value
        if state:
            rs = await obj.client._request(
                action="get_value", path=state + r.path, iter=False, nchain=obj.meta
            )
            if obj.meta:
                val["state"] = rs
            elif "value" in rs:
                val["state"] = rs.value
            rs = rs.value
            try:
                rs.started_date = datetime.datetime.fromtimestamp(rs.started).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                pass
            try:
                rs.stopped_date = datetime.datetime.fromtimestamp(rs.stopped).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                pass
        try:
            val.target_date = datetime.datetime.fromtimestamp(val.target).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except AttributeError:
            pass

        return r

    await data_get(obj, path, as_dict=as_dict, item_mangle=state_getter)


@cli.command("state")
@click.option("-r", "--result", is_flag=True, help="Just print the actual result.")
@click.argument("path", nargs=1)
@click.pass_obj
async def state_(obj, path, result):
    """Get the status of a runner entry.
    """
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if result and obj.meta:
        raise click.UsageError("You can't use '-v' and '-r' at the same time.")
    if not len(path):
        raise click.UsageError("You need a non-empty path.")
    path = obj.statepath + P(path)

    res = await obj.client.get(path, nchain=obj.meta)
    if "value" not in res:
        if obj.debug:
            print("Not found (yet?)", file=sys.stderr)
        sys.exit(1)

    if not obj.meta:
        res = res.value
    yprint(res, stream=obj.stdout)


@cli.command()
@click.argument("path", nargs=1)
@click.pass_obj
async def get(obj, path):
    """Read a runner entry"""
    path = P(path)
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if not path:
        raise click.UsageError("You need a non-empty path.")
    path = obj.path + path

    res = await obj.client._request(action="get_value", path=path, iter=False, nchain=obj.meta)
    if not obj.meta:
        res = res.value

    yprint(res, stream=obj.stdout)


@cli.command("set")
@click.option("-c", "--code", help="Path to the code that should run.")
@click.option("-t", "--time", "tm", help="time the code should next run at. '-':not")
@click.option("-r", "--repeat", type=int, help="Seconds the code should re-run after")
@click.option("-k", "--ok", type=int, help="Code is OK if it ran this many seconds")
@click.option("-b", "--backoff", type=float, help="Back-off factor. Default: 1.4")
@click.option("-d", "--delay", type=int, help="Seconds the code should retry after (w/ backoff)")
@click.option("-i", "--info", help="Short human-readable information")
@click.option("-v", "--var", nargs=2, help="Value (name val…)")
@click.option("-e", "--eval", "eval_", is_flag=True, help="Value must be evaluated")
@click.option("-p", "--path", "path_", is_flag=True, help="Value is a path")
@click.argument("path", nargs=1)
@click.pass_obj
async def set_(obj, path, code, tm, info, ok, repeat, delay, backoff, eval_, path_, var):
    """Save / modify a run entry."""
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if path_ and eval_:
        raise click.UsageError("'--eval' and '--path' are mutually exclusive.")
    if (path_ or eval_) and not var:
        raise click.UsageError("'--eval' or '--path' need a variable+value.")

    if code is not None:
        code = P(code)
    path = obj.path + P(path)

    try:
        res = await obj.client._request(action="get_value", path=path, iter=False, nchain=3)
        if "value" not in res:
            raise ServerError
    except ServerError:
        if code is None:
            raise click.UsageError("New entry, need code")
        res = {}
        chain = None
    else:
        chain = res["chain"]
        res = res["value"]

    if var:
        vl = res.setdefault("data", {})
        k, v = var
        if eval_:
            v = eval(v)  # pylint:disable=eval-used
        elif path_:
            v = P(v)
        vl[k] = v
    if code is not None:
        res["code"] = code
    if ok is not None:
        res["ok_after"] = ok
    if info is not None:
        res["info"] = info
    if backoff is not None:
        res["backoff"] = backoff
    if delay is not None:
        res["delay"] = delay
    if repeat is not None:
        res["repeat"] = repeat
    if tm is not None:
        if tm == "-":
            res["target"] = None
        else:
            res["target"] = time.time() + float(tm)

    res = await obj.client.set(path, value=res, nchain=3, **({"chain": chain} if obj.meta else {}))
    if obj.meta:
        yprint(res, stream=obj.stdout)


@click.command()
@click.pass_obj
async def monitor(obj):
    """
    Runners periodically send a keepalive message to Serf, if thus
    configured (this is the default).
    """

    # TODO this does not watch changes in DistKV.
    # It also should watch individual jobs' state changes.
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")

    async with obj.client.msg_monitor("run") as cl:
        async for msg in cl:
            yprint(msg, stream=obj.stdout)
            print("---", file=obj.stdout)
