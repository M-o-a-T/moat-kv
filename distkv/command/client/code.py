# command line interface

import os
import sys
import asyncclick as click
import yaml

from distkv.util import (
    attrdict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.client import StreamedRequest
from distkv.command import Loader
from distkv.default import CFG
from distkv.server import Server
from distkv.auth import loader, gen_auth
from distkv.exceptions import ClientError
from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)


@main.group()
@click.pass_obj
async def cli(obj):
    """Manage code stored in DistKV."""
    pass


@cli.command()
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, script):
    """Read a code entry"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value",
        path=obj.cfg["codes"]["prefix"] + path,
        iter=False,
        nchain=obj.meta,
    )
    if not obj.meta:
        res = res.value
    if script:
        code = res.pop("code", None)
        if code is not None:
            print(code, file=script)
    yprint(res, file=obj.stdout)


@cli.command()
@click.option("-a", "--async", "async_", is_flag=True, help="The code is async")
@click.option(
    "-t", "--thread", is_flag=True, help="The code should run in a worker thread"
)
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the code")
@click.option(
    "-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, thread, script, data, async_):
    """Save Python code."""
    if async_:
        if thread:
            raise click.UsageError("You can't specify both '--async' and '--thread'.")
    else:
        if thread:
            async_ = False
        else:
            async_ = None

    if not path:
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yaml.safe_load(data)
    else:
        msg = {}
    chain = None
    if "value" in msg:
        chain = msg.get("chain", None)
        msg = msg["value"]
    if async_ is not None or "is_async" not in msg:
        msg["is_async"] = async_

    if "code" in msg:
        if script:
            raise click.UsageError("Duplicate script")
    else:
        if not script:
            raise click.UsageError("Missing script")
        msg["code"] = script.read()

    res = await obj.client.set(
        *obj.cfg["codes"]["prefix"],
        *path,
        value=msg,
        iter=False,
        nchain=obj.meta,
        chain=chain,
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.group("module")
@click.pass_obj
async def mod(obj):
    """
    Change the code of a module stored in DistKV
    """


@mod.command()
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, script):
    """Read a module entry"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value",
        path=obj.cfg["modules"]["prefix"] + path,
        iter=False,
        nchain=obj.meta,
    )
    if not obj.meta:
        res = res.value
    if script:
        code = res.pop("code", None)
        if code is not None:
            print(code, file=script)

    yprint(res, stream=obj.stdout)


@mod.command()
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the module's code"
)
@click.option(
    "-d", "--data", type=click.File(mode="r"), help="load the metadata (YAML)"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, script, data):
    """Save a Python module to DistKV."""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yaml.safe_load(data)
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
        *obj.cfg["modules"]["prefix"],
        *path,
        value=msg,
        iter=False,
        nchain=obj.meta,
        chain=chain,
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)
