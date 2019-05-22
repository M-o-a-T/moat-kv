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
    """Manage codecs and converters. Usage: … codec …"""
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
    "-e", "--encode", type=click.File(mode="w", lazy=True), help="Save the encoder here"
)
@click.option(
    "-d", "--decode", type=click.File(mode="w", lazy=True), help="Save the decoder here"
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the data here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, yaml, verbose, script, encode, decode):
    """Read type information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal",
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if encode and res.get("encode", None) is not None:
        encode.write(res.pop("encode"))
    if decode and res.get("decode", None) is not None:
        decode.write(res.pop("decode"))

    if not verbose:
        res = res.value
    if yaml:
        import yaml

        print(yaml.safe_dump(res, default_flow_style=False), file=script or sys.stdout)
    else:
        pprint(res)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-e", "--encode", type=click.File(mode="r"), help="File with the encoder")
@click.option("-d", "--decode", type=click.File(mode="r"), help="File with the decoder")
@click.option("-s", "--script", type=click.File(mode="r"), help="File with the rest")
@click.option("-y", "--yaml", is_flag=True, help="load everything from this file")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, verbose, encode, decode, script, yaml):
    """Save codec information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    if "encode" not in msg:
        if not encode:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to encode 'value'.")
            encode = sys.stdin.read()
        else:
            encode = encode.read()
        msg["encode"] = encode
    elif encode and not yaml:
        raise click.UsageError("Duplicate encode parameter")
    if "decode" not in msg:
        if not decode:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to decode 'value'.")
            decode = sys.stdin.read()
        else:
            decode = decode.read()
        msg["decode"] = decode
    elif decode and not yaml:
        raise click.UsageError("Duplicate decode parameter")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("codec",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if verbose:
        pprint(res)
    elif obj.verbose:
        print(res.tock)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-c", "--codec", multiple=True, help="Codec to link to. Multiple for hierarchical."
)
@click.option("-d", "--delete", help="Use to delete this converter.")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def convert(obj, path, codec, name, delete, verbose):
    """Match a codec to a path (read, if no codec given)"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if codec and delete:
        raise click.UsageError("You can't both set and delete a path.")

    if delete:
        res = await obj.client._request(
            action="delete_internal", path=("conv", name) + path
        )
    else:
        msg = {"codec": codec}
        res = await obj.client._request(
            action="set_internal",
            value=msg,
            path=("conv", name) + path,
            iter=False,
            nchain=3 if verbose else 0,
        )
    if verbose:
        pprint(res)
    elif type or delete:
        print(res.tock)
    else:
        print(" ".join(res.type))
