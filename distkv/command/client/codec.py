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
    """Manage codecs and converters. Usage: … codec …"""
    pass


@cli.command()
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
async def get(obj, path, script, encode, decode):
    """Read type information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal", path=("type",) + path, iter=False, nchain=obj.meta
    )
    if encode and res.get("encode", None) is not None:
        encode.write(res.pop("encode"))
    if decode and res.get("decode", None) is not None:
        decode.write(res.pop("decode"))

    if not obj.meta:
        res = res.value
    yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-e", "--encode", type=click.File(mode="r"), help="File with the encoder")
@click.option("-d", "--decode", type=click.File(mode="r"), help="File with the decoder")
@click.option("-d", "--data", type=click.File(mode="r"), help="File with the rest")
@click.option("-i", "--in", "in_", nargs=2, multiple=True, help="Decoding sample")
@click.option("-o", "--out", nargs=2, multiple=True, help="Encoding sample")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, encode, decode, data, in_, out):
    """Save codec information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yaml.safe_load(data)
    else:
        msg = {}
    if "encode" in msg:
        if encode:
            raise click.UsageError("Duplicate encode script")
    else:
        if not encode:
            raise click.UsageError("Missing encode script")
        msg["encode"] = encode.read()
    if "decode" in msg:
        if decode:
            raise click.UsageError("Duplicate decode script")
    else:
        if not decode:
            raise click.UsageError("Missing decode script")
        msg["decode"] = decode.read()
    if in_:
        msg["in"] = [(eval(a), eval(b)) for a, b in in_]
    if out:
        msg["out"] = [(eval(a), eval(b)) for a, b in out]

    if not msg["in"]:
        raise click.UsageError("Missing decode tests")
    if not msg["out"]:
        raise click.UsageError("Missing encode tests")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("codec",) + path,
        iter=False,
        nchain=obj.meta,
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command()
@click.option(
    "-c", "--codec", multiple=True, help="Codec to link to. Multiple for hierarchical."
)
@click.option("-d", "--delete", help="Use to delete this converter.")
@click.argument("name", nargs=1)
@click.argument("path", nargs=-1)
@click.pass_obj
async def convert(obj, path, codec, name, delete):
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
            nchain=obj.meta,
        )
    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif type or delete:
        print(res.tock, file=obj.stdout)
    else:
        print(" ".join(res.type), file=obj.stdout)
