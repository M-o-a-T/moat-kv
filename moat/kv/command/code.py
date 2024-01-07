# command line interface

import sys

import asyncclick as click
from moat.util import (
    NotGiven,
    P,
    Path,
    PathLongener,
    attr_args,
    process_args,
    yload,
    yprint,
)


@click.group(invoke_without_command=True)  # pylint: disable=undefined-variable
@click.argument("path", nargs=1, type=P)
@click.pass_context
async def cli(ctx, path):
    """Manage code stored in MoaT-KV."""
    obj = ctx.obj
    obj.path = obj.cfg["kv"]["codes"]["prefix"] + path
    obj.codepath = path

    if ctx.invoked_subcommand is None:
        pl = PathLongener(path)
        async for res in obj.client.get_tree(obj.path, long_path=False):
            pl(res)
            print(Path(*res.path), res.value.info, file=obj.stdout)


@cli.command()
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.pass_obj
async def get(obj, script):
    """Read a code entry"""
    if not len(obj.codepath):
        raise click.UsageError("You need a non-empty path.")

    res = await obj.client._request(
        action="get_value", path=obj.path, iter=False, nchain=obj.meta
    )
    if "value" not in res:
        if obj.debug:
            print("No entry here.", file=sys.stderr)
        sys.exit(1)
    if not obj.meta:
        res = res.value
    if script:
        code = res.pop("code", None)
        if code is not None:
            print(code, file=script)
    yprint(res, stream=obj.stdout)


@cli.command("set")
@click.option(
    "-a/-A",
    "--async/--sync",
    "async_",
    is_flag=True,
    help="The code is async / sync (default: async)",
    default=True,
)
@click.option(
    "-t", "--thread", is_flag=True, help="The code should run in a worker thread"
)
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the code")
@click.option("-i", "--info", type=str, help="one-liner info about the code")
@click.option(
    "-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)"
)
@attr_args
@click.pass_obj
async def set_(obj, thread, script, data, vars_, eval_, path_, async_, info):
    """Save Python code.

    The code may have inputs. You specify the inputs and their default
    values with '-v VAR VALUE' (string), '-p VAR PATH' (MoaT-KV path), or
    '-e VAR EXPR' (simple Python expression). Use '-e VAR -' to state that
    VAR shall not have a default value, and '-e VAR /' to delete VAR from
    the list of inputs entirely.
    """
    if thread:
        async_ = False
    elif not async_:
        async_ = None

    if not len(obj.codepath):
        raise click.UsageError("You need a non-empty path.")
    path = obj.path

    if data:
        msg = yload(data)
    else:
        msg = await obj.client.get(path, nchain=3)
    chain = NotGiven
    if "value" in msg:
        chain = msg.get("chain", NotGiven)
        msg = msg["value"]
    if async_ is not None or "is_async" not in msg:
        msg["is_async"] = async_

    if info is not None:
        msg["info"] = info

    if script:
        msg["code"] = script.read()
    elif "code" not in msg:
        raise click.UsageError("Missing script")

    if "vars" in msg:
        vs = set(msg["vars"])
    else:
        vs = set()
    vd = msg.setdefault("default", {})

    vd = process_args(vd, vars_, eval_, path_, vs=vs)
    msg["vars"] = list(vs)
    msg["default"] = vd

    kv = {}
    if chain is not NotGiven:
        kv["chain"] = chain

    res = await obj.client.set(obj.path, value=msg, nchain=obj.meta, **kv)
    if obj.meta:
        yprint(res, stream=obj.stdout)


# disabled for now
@cli.group("module", hidden=True)
async def mod():
    """
    Change the code of a module stored in MoaT-KV
    """


@mod.command("get")
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.argument("path", nargs=1)
@click.pass_obj  # pylint: disable=function-redefined
async def get_mod(obj, path, script):
    """Read a module entry"""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value",
        path=obj.cfg["kv"]["modules"]["prefix"] + path,
        iter=False,
        nchain=obj.meta,
    )
    if not obj.meta:
        res = res.value

    code = res.pop("code", None)
    if code is not None:
        code = code.rstrip("\n \t") + "\n"
        if script:
            print(code, file=script)
        else:
            res["code"] = code

    yprint(res, stream=obj.stdout)


@mod.command("set")
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the module's code"
)
@click.option(
    "-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)"
)
@click.argument("path", nargs=1)  # pylint: disable=function-redefined
@click.pass_obj
async def set_mod(obj, path, script, data):
    """Save a Python module to MoaT-KV."""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yload(data)
    else:
        msg = {}
    chain = None
    if "value" in msg:
        chain = msg.get("chain", None)
        msg = msg["value"]

    if "code" not in msg:
        if script:
            raise click.UsageError("Duplicate script")
    else:
        if not script:
            raise click.UsageError("Missing script")
        msg["code"] = script.read()

    res = await obj.client.set(
        *obj.cfg["kv"]["modules"]["prefix"],
        *path,
        value=msg,
        iter=False,
        nchain=obj.meta,
        chain=chain,
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)


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
    default=None,
    help="Limit recursion depth. Default: whole tree",
)
@click.option(
    "-M",
    "--mindepth",
    type=int,
    default=None,
    help="Starting depth. Default: whole tree",
)
@click.option("-f", "--full", is_flag=True, help="print complete entries.")
@click.option("-s", "--short", is_flag=True, help="print shortened entries.")
@click.pass_obj
async def list_(obj, as_dict, maxdepth, mindepth, full, short):
    """
    List code entries.

    Be aware that the whole subtree will be read before anything is
    printed if you use the `--as-dict` option.
    """

    if (full or as_dict) and short:
        raise click.UsageError("'-f'/'-d' and '-s' are incompatible.")
    kw = {}
    if maxdepth is not None:
        kw["max_depth"] = maxdepth
    if mindepth is not None:
        kw["min_depth"] = mindepth
    y = {}
    async for r in obj.client.get_tree(obj.path, nchain=obj.meta, **kw):
        r.pop("seq", None)
        path = r.pop("path")
        if not full:
            if "info" not in r.value:
                r.value.info = f"<{len(r.value.code.splitlines())} lines>"
            del r.value["code"]

        if short:
            print(path, "::", r.value.info)
            continue

        if as_dict is not None:
            yy = y
            for p in path:
                yy = yy.setdefault(p, {})
            try:
                yy[as_dict] = r if obj.meta else r.value
            except AttributeError:
                continue
        else:
            y = {}
            try:
                y[path] = r if obj.meta else r.value
            except AttributeError:
                continue
            yprint([y], stream=obj.stdout)

    if as_dict is not None:
        yprint(y, stream=obj.stdout)
    return


@cli.command()
@click.pass_obj
async def delete(obj):
    """Remove a code entry"""
    res = await obj.client.get(obj.path, nchain=3)
    if "value" not in res:
        res.info = "Does not exist."
    else:
        res = await obj.client.delete(obj.path, chain=res.chain)
        res.info = "Deleted."

    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif obj.debug:
        print(res.info, file=obj.stdout)
