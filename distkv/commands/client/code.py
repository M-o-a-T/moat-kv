# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

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


@main.group()
@click.pass_obj
async def cli(obj):
    """Manage code stored in DistKV."""
    pass


@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, yaml, verbose, script):
    """Read a code entry"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value",
        path=obj.cfg['codes']['prefix'] + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if not verbose:
        res = res.value
    if script:
        code = res.pop("code", None)
        if code is not None:
            print(code, file=script)
    pprint(res)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-a", "--async", is_flag=True, help="The code is async")
@click.option("-t", "--thread", is_flag=True, help="The code should run in a worker thread")
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the code"
)
@click.option("-y", "--yaml", is_flag=True, help="load everything from the 'script' file")
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, chain, thread, verbose, script, yaml, **kw):
    """Save Python code."""
    if kw.get('async', False):
        if thread:
            raise click.UsageError("You can't specify both '--async' and '--thread'.")
        else:
            async_ = True
    else:
        if thread:
            async_ = False
        else:
            async_ = None

    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    if "value" in msg:
        chain = msg.get('chain', chain)
        msg = msg['value']
    if async_ is not None or 'is_async' not in msg:
        msg['is_async'] = async_

    if "code" not in msg:
        if not script:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to run here.")
            script = sys.stdin.read()
        else:
            script = script.read()
        msg["code"] = script
    elif script and not yaml:
        raise click.UsageError("Duplicate script parameter")

    res = await obj.client._request(
        action="set_value",
        value=msg,
        path=obj.cfg['codes']['prefix'] + path,
        iter=False,
        nchain=3 if verbose else 0,
        **({"chain":chain} if chain else {})
    )
    if verbose:
        if yaml:
            print(yaml.safe_dump(res, default_flow_style=False))
        else:
            print(res.tock)


@cli.group('module')
@click.pass_obj
async def mod(obj):
    """
    Change the code of a module stored in DistKV
    """

@mod.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the code here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, yaml, verbose, script):
    """Read a module entry"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_value",
        path=obj.cfg['modules']['prefix'] + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if not verbose:
        res = res.value
    if script:
        code = res.pop("code", None)
        if code is not None:
            print(code, file=script)
    pprint(res)


@mod.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the module's code"
)
@click.option("-y", "--yaml", is_flag=True, help="load everything from the 'script' file")
@click.option(
    "-c",
    "--chain",
    type=int,
    default=0,
    help="Length of change list to return. Default: 0",
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, chain, verbose, script, yaml):
    """Save a Python module to DistKV."""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    if "value" in msg:
        chain = msg.get('chain', chain)
        msg = msg['value']

    if "code" not in msg:
        if not script:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python module here.")
            script = sys.stdin.read()
        else:
            script = script.read()
        msg["code"] = script
    elif script and not yaml:
        raise click.UsageError("Duplicate script parameter")

    res = await obj.client._request(
        action="set_value",
        value=msg,
        path=obj.cfg['modules']['prefix'] + path,
        iter=False,
        nchain=3 if verbose else 0,
        **({"chain":chain} if chain else {})
    )
    if verbose:
        if yaml:
            print(yaml.safe_dump(res, default_flow_style=False))
        else:
            print(res.tock)
