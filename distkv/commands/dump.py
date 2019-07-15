# command line interface

import os
import sys
import trio_click as click
from distkv.util import MsgReader

from distkv.util import (
    attrdict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.client import open_client, StreamedRequest
from distkv.default import CFG
from distkv.server import Server
from distkv.auth import loader, gen_auth
from distkv.exceptions import ClientError
from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage data.")
@click.pass_obj
async def cli(obj):
    """
    This subcommand accesses the actual user data stored in your DistKV tree.
    """
    pass


@cli.command()
@click.pass_obj
async def cfg(obj):
    """emit the current configuration as a YAML file."""
    yprint(obj.cfg, stream=obj.stdout)


@cli.command()
@click.argument("file", nargs=1)
@click.pass_obj
async def file(obj, file):
    """Read a MsgPack file and dump as YAML."""
    async with MsgReader(path=file) as f:
        async for msg in f:
            yprint(msg, stream=obj.stdout)
            print("---", file=obj.stdout)
