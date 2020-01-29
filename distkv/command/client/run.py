# command line interface

import sys
import asyncclick as click
import time
import anyio

from distkv.exceptions import ServerError
from distkv.code import CodeRoot
from distkv.runner import AnyRunnerRoot, SingleRunnerRoot, AllRunnerRoot
from distkv.util import yprint

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
    elif node == '-':
        obj.runner_root = AllRunnerRoot
        subpath = (group,)
    else:
        obj.runner_root = SingleRunnerRoot
        subpath = (group, node)

    obj.subpath = (obj.cfg["runner"]["sub"][obj.runner_root.SUB],) + subpath
    obj.path = obj.cfg["runner"]["prefix"] + obj.subpath
    obj.statepath = obj.cfg["runner"]["state"] + obj.subpath


@cli.command()
@click.pass_obj
async def all(obj):
    """
    Run code that needs to run.

    This does not return.
    """
    from distkv.util import as_service

    async with as_service(obj) as evt:
        _, evt = evt
        c = obj.client
        cr = await CodeRoot.as_handler(c)
        r = await obj.runner_root.as_handler(c, subpath=obj.subpath, code=cr)
        await evt.set()
        while True:
            await anyio.sleep(99999)


@cli.command()
@click.option("-s", "--state", is_flag=True, help="Add state data")
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def list(obj, state, as_dict, path):
    """List run entries.
    """
    if not path:
        path = ()
    path = obj.path+path
    if state:
        state = obj.statepath+path
    res = await obj.client._request(
        action="get_tree", path=path, iter=True, nchain=obj.meta
    )

    y = {}
    async for r in res:
        if as_dict is not None:
            yy = y
            for p in r.pop("path"):
                yy = yy.setdefault(p, {})
            yy[as_dict] = r if obj.meta else r.pop("value")
        else:
            yy = {}
            if obj.meta:
                yy[r.pop("path")] = r
            else:
                yy[r.path] = r.value

        if state:
            rs = await obj.client._request(
                action="get_value", path=state, iter=False, nchain=obj.meta
            )
            if "value" in rs:
                if not obj.meta:
                    rs = rs.value
                yy["state"] = rs
            else:
                yy["state"] = None
        if as_dict is None:
            yprint([yy], stream=obj.stdout)

    if as_dict is not None:
        yprint(y, stream=obj.stdout)


@cli.command()
@click.option("-r", "--result", is_flag=True, help="Just print the actual result.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def state(obj, path, result):
    """Get the status of a runner entry.
    """
    if result and obj.meta:
        raise click.UsageError("You can't use '-v' and '-r' at the same time.")
    if not path:
        raise click.UsageError("You need a non-empty path.")
    path = obj.statepath + path

    res = await obj.client._request(
        action="get_value", path=path, iter=False, nchain=obj.meta
    )
    if "value" not in res:
        if obj.debug:
            print("Not found (yet?)", file=sys.stderr)
        sys.exit(1)
    if not obj.meta:
        res = res.value

    yprint(res, stream=obj.stdout)


@cli.command()
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path):
    """Read a runner entry"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    path = obj.path+ path

    res = await obj.client._request(
        action="get_value", path=path, iter=False, nchain=obj.meta
    )
    if not obj.meta:
        res = res.value

    yprint(res, stream=obj.stdout)


@cli.command()
@click.option(
    "-c", "--code", help="Path to the code that should run. Space separated path."
)
@click.option("-t", "--time", "tm", type=float, help="time the code should next run at")
@click.option("-r", "--repeat", type=int, help="Seconds the code should re-run after")
@click.option("-k", "--ok", type=int, help="Code is OK if it ran this many seconds")
@click.option("-b", "--backoff", type=float, help="Back-off factor. Default: 1.4")
@click.option(
    "-d", "--delay", type=int, help="Seconds the code should retry after (w/ backoff)"
)
@click.option("-i", "--info", help="Short human-readable information")
@click.option(
    "-e", "--eval", "eval_", help="'code' is a Python expression (must eval to a list)"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, code, eval_, tm, info, ok, repeat, delay, backoff):
    """Save / modify a run entry."""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if eval_:
        code = eval(code)
        if not isinstance(code, (list, tuple)):
            raise click.UsageError("'code' must be a list")
    elif code is not None:
        code = code.split(" ")

    path = obj.path + path

    try:
        res = await obj.client._request(
            action="get_value", path=path, iter=False, nchain=3
        )
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
        res["target"] = time.time() + tm

    res = await obj.client.set(
        *path,
        value=res,
        nchain=3,
        **({"chain": chain} if obj.meta else {})
    )
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

    async with obj.client.msg_monitor("run") as cl:
        async for msg in cl:
            yprint(msg, stream=obj.stdout)
            print("---", file=obj.stdout)
            await process_test(msg.data)

