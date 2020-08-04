# command line interface

import sys
import asyncclick as click
import time
import anyio
import datetime
from functools import partial

from distkv.code import CodeRoot
from distkv.runner import AnyRunnerRoot, SingleRunnerRoot, AllRunnerRoot
from distkv.util import yprint, PathLongener, P, Path, data_get

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
@click.option("-n", "--node", help="node to run this code on. Empty: any one node, '-': all nodes")
@click.option("-g", "--group", help="group to run this code on. Empty: default")
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
    res = dict(command=obj.path + path, state=obj.statepath + path)
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


def _state_fix_2(rs):
    try:
        if rs.started:
            rs.started_date = datetime.datetime.fromtimestamp(rs.started).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    except AttributeError:
        pass
    try:
        if rs.stopped:
            rs.stopped_date = datetime.datetime.fromtimestamp(rs.stopped).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    except AttributeError:
        pass
    try:
        if rs.computed:
            try:
                rs.computed_date = datetime.datetime.fromtimestamp(rs.computed).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except (ValueError, OverflowError):
                pass
    except AttributeError:
        pass


async def _state_fix(obj, state, path, r):
    try:
        val = r.value
    except AttributeError:
        return
    if state:
        rs = await obj.client._request(
            action="get_value", path=state + r.path, iter=False, nchain=obj.meta
        )
        if obj.meta:
            val["state"] = rs
        elif "value" in rs:
            val["state"] = rs.value
        if "value" in rs:
            _state_fix_2(rs.value)
    if path:
        r.path = path + r.path
    try:
        val.target_date = datetime.datetime.fromtimestamp(val.target).strftime("%Y-%m-%d %H:%M:%S")
    except (AttributeError, TypeError):
        pass

    return r


@cli.command("list")
@click.option("-s", "--state", is_flag=True, help="Add state data")
@click.option("-t", "--table", is_flag=True, help="one-line output")
@click.option(
    "-d",
    "--as-dict",
    default=None,
    help="Structure as dictionary. The argument is the key to use "
    "for values. Default: return as list",
)
@click.argument("path", nargs=1)
@click.pass_obj
async def list_(obj, state, table, as_dict, path):
    """List run entries.
    """
    if table and state:
        click.UsageError("'--table' and '--state' are mutually exclusive")

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

    if state or table:
        state = obj.statepath + path

    if table:
        from distkv.errors import ErrorRoot

        err = await ErrorRoot.as_handler(obj.client)

        async for r in obj.client.get_tree(obj.path + path):
            p = path + r.path
            s = await obj.client.get(state + r.path)
            if "value" not in s:
                st = "-never-"
            elif s.value.started > s.value.stopped:
                st = s.value.node
            else:
                try:
                    e = await err.get_error_record("run", obj.path + p, create=False)
                except KeyError:
                    st = "-stopped-"
                else:
                    if e is None or e.resolved:
                        st = "-stopped-"
                    else:
                        st = " | ".join(
                            "%s %s"
                            % (
                                Path.build(e.subpath)
                                if e._path[-2] == ee._path[-1]
                                else Path.build(ee.subpath),
                                getattr(ee, "message", None)
                                or getattr(ee, "comment", None)
                                or "-",
                            )
                            for ee in e
                        )
            print(p, r.value.code, st, file=obj.stdout)

    else:
        await data_get(
            obj,
            obj.path + path,
            as_dict=as_dict,
            item_mangle=partial(_state_fix, obj, state, None if as_dict else path),
        )


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

    _state_fix_2(res.value)
    if not obj.meta:
        res = res.value
    yprint(res, stream=obj.stdout)


@cli.command()
@click.argument("path", nargs=1)
@click.option("-s", "--state", is_flag=True, help="Add state data")
@click.pass_obj
async def get(obj, path, state):
    """Read a runner entry"""
    path = P(path)
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if not path:
        raise click.UsageError("You need a non-empty path.")

    res = await obj.client._request(
        action="get_value", path=obj.path + path, iter=False, nchain=obj.meta
    )
    if "value" not in res:
        print("Not found.", file=sys.stderr)
        return
    res.path = path
    if state:
        state = obj.statepath
    await _state_fix(obj, state, res, None)
    if not obj.meta:
        res = res.value

    yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-f", "--force", is_flag=True, help="Force deletion even if messy")
@click.argument("path", nargs=1)
@click.pass_obj
async def delete(obj, path, force):
    """Remove a runner entry"""
    path = P(path)
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")
    if not path:
        raise click.UsageError("You need a non-empty path.")

    res = await obj.client.get(obj.path + path, nchain=3)
    if "value" not in res:
        res.info = "Does not exist."
    else:
        val = res.value
        if val.target is not None:
            val.target = None
            res = await obj.client.set(obj.path + path, value=val, nchain=3, chain=res.chain)
            if not force:
                res.info = "'target' was set: cleared but not deleted."
        if force or val.target is None:
            sres = await obj.client.get(obj.statepath + path, nchain=3)
            if not force and "value" in sres and sres.value.stopped < sres.value.started:
                res.info = "Still running, not deleted."
            else:
                sres = await obj.client.delete(obj.statepath + path, chain=sres.chain)
                res = await obj.client.delete(obj.path + path, chain=res.chain)
                if "value" in res and res.value.stopped < res.value.started:
                    res.info = "Deleted (unclean!)."
                else:
                    res.info = "Deleted."

    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif obj.debug:
        print(res.info)


@cli.command("set")
@click.option("-c", "--code", help="Path to the code that should run.")
@click.option("-C", "--copy", help="Use this entry as a template.")
@click.option("-t", "--time", "tm", help="time the code should next run at. '-':not")
@click.option("-r", "--repeat", type=int, help="Seconds the code should re-run after")
@click.option("-k", "--ok", type=float, help="Code is OK if it ran this many seconds")
@click.option("-b", "--backoff", type=float, help="Back-off factor. Default: 1.4")
@click.option("-d", "--delay", type=int, help="Seconds the code should retry after (w/ backoff)")
@click.option("-i", "--info", help="Short human-readable information")
@click.option("-v", "--var", nargs=2, multiple=True, help="Value (name val…)")
@click.option("-e", "--eval", "eval_", nargs=2, multiple=True, help="Value (name val), evaluated")
@click.option("-p", "--path", "path_", nargs=2, multiple=True, help="Value (name val), as path")
@click.argument("path", nargs=1)
@click.pass_obj
async def set_(obj, path, code, tm, info, ok, repeat, delay, backoff, eval_, path_, var, copy):
    """Add or modify a runner.

    Code typically requires some input parameters.

    You should use '-v NAME VALUE' for string values, '-p NAME VALUE' for
    paths, and '-e NAME VALUE' for other data. '-e NAME -' deletes an item.
    """
    if obj.subpath[-1] == "-":
        raise click.UsageError("Group '-' can only be used for listing.")

    if code is not None:
        code = P(code)
    if copy:
        copy = P(copy)
    path = obj.path + P(path)

    res = await obj.client._request(action="get_value", path=copy or path, iter=False, nchain=3)
    if "value" not in res:
        if copy:
            raise click.UsageError("--copy: use the complete path to an existing entry")
        elif code is None:
            raise click.UsageError("New entry, need code")
        res = {}
        chain = None
    else:
        chain = None if copy else res["chain"]
        res = res["value"]
        if copy and "code" not in res:
            raise click.UsageError("'--copy' needs a runner entry")

    vl = res.setdefault("data", {})
    for k, v in var:
        vl[k] = v
    for k, v in eval_:
        if v == "-":
            vl.pop(k, None)
        else:
            vl[k] = eval(v)  # pylint:disable=eval-used
    for k, v in path_:
        vl[k] = P(v)

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

    res = await obj.client.set(path, value=res, nchain=3, chain=chain)
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
