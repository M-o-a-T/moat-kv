# command line interface

import asyncclick as click
import sys

from distkv.util import yprint, NotGiven, yload, P

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
async def cli():
    """Manage code stored in DistKV."""
    pass


@cli.command()
@click.option("-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here")
@click.argument("path", nargs=1)
@click.pass_obj
async def get(obj, path, script):
    """Read a code entry"""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value", path=obj.cfg["codes"]["prefix"] + path, iter=False, nchain=obj.meta
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
@click.option("-a", "--async", "async_", is_flag=True, help="The code is async")
@click.option("-t", "--thread", is_flag=True, help="The code should run in a worker thread")
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the code")
@click.option("-i", "--info", type=str, help="one-liner info about the code")
@click.option("-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)")
@click.option("-v", "--var", "vars_", nargs=2, multiple=True, help="Value (name val…)")
@click.option("-e", "--eval", "eval_", nargs=2, multiple=True, help="Value (name val), evaluated")
@click.option("-p", "--path", "path_", nargs=2, multiple=True, help="Value (name val), as path")
@click.argument("path", nargs=1)
@click.pass_obj
async def set_(obj, path, thread, script, data, vars_, eval_, path_, async_, info):
    """Save Python code.

    The code may have inputs. You specify the inputs and their default
    values with '-v VAR VALUE' (string), '-p VAR PATH' (DistKV path), or
    '-e VAR EXPR' (Python expression). Use '-e VAR -' to state that VAR
    shall not have a default value, and '-e VAR /' to delete VAR from the
    list of inputs entirely.
    """
    if async_:
        if thread:
            raise click.UsageError("You can't specify both '--async' and '--thread'.")
    else:
        if thread:
            async_ = False
        else:
            async_ = None

    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yload(data)
    else:
        msg = await obj.client.get(obj.cfg["codes"]["prefix"] + path, nchain=3)
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

    for k, v in vars_:
        vs.add(k)
        vd[k] = v
    for k, v in eval_:
        vs.add(k)
        if v == "-":
            vd.pop(k, None)
        elif v == "/":
            vd.pop(k, None)
            vs.discard(k)
        else:
            vd[k] = eval(v)  # pylint:disable=eval-used
    for k, v in path_:
        vs.add(k)
        vd[k] = P(v)
    msg["vars"] = list(vs)

    res = await obj.client.set(
        obj.cfg["codes"]["prefix"] + path,
        value=msg,
        nchain=obj.meta,
        **({"chain": chain} if chain is not NotGiven else {}),
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.group("module")
async def mod():
    """
    Change the code of a module stored in DistKV
    """


@mod.command("get")
@click.option("-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here")
@click.argument("path", nargs=1)
@click.pass_obj  # pylint: disable=function-redefined
async def get_mod(obj, path, script):
    """Read a module entry"""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value", path=obj.cfg["modules"]["prefix"] + path, iter=False, nchain=obj.meta
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
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the module's code")
@click.option("-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)")
@click.argument("path", nargs=1)  # pylint: disable=function-redefined
@click.pass_obj
async def set_mod(obj, path, script, data):
    """Save a Python module to DistKV."""
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
        *obj.cfg["modules"]["prefix"], *path, value=msg, iter=False, nchain=obj.meta, chain=chain
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
    "-m", "--maxdepth", type=int, default=None, help="Limit recursion depth. Default: whole tree"
)
@click.option(
    "-M", "--mindepth", type=int, default=None, help="Starting depth. Default: whole tree"
)
@click.option("-f", "--full", is_flag=True, help="print complete entries.")
@click.option("-s", "--short", is_flag=True, help="print shortened entries.")
@click.argument("path", nargs=1)
@click.pass_obj
async def list_(obj, path, as_dict, maxdepth, mindepth, full, short):
    """
    List code entries.

    Be aware that the whole subtree will be read before anything is
    printed if you use the `--as-dict` option.
    """

    path = P(path)
    if (full or as_dict) and short:
        raise click.UsageError("'-f'/'-d' and '-s' are incompatible.")
    kw = {}
    if maxdepth is not None:
        kw["max_depth"] = maxdepth
    if mindepth is not None:
        kw["min_depth"] = mindepth
    y = {}
    async for r in obj.client.get_tree(obj.cfg["codes"].prefix + path, nchain=obj.meta, **kw):
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
@click.argument("path", nargs=1)
@click.pass_obj
async def delete(obj, path):
    """Remove a code entry"""
    path = P(path)

    res = await obj.client.get(obj.cfg["codes"].prefix + path, nchain=3)
    if "value" not in res:
        res.info = "Does not exist."
    else:
        res = await obj.client.delete(obj.cfg["codes"].prefix + path, chain=res.chain)
        res.info = "Deleted."

    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif obj.debug:
        print(res.info, file=obj.stdout)
