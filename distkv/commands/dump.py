# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json
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
    import yaml
    yaml.safe_dump(obj.cfg, stream=sys.stdout)

@cli.command()
@click.argument("file", nargs=1)
@click.pass_obj
async def file(obj, file):
    """Read a MsgPack file and dump as YAML."""
    import yaml

    async with MsgReader(path=file) as f:
        async for msg in f:
            yaml.safe_dump(msg, stream=sys.stdout)
            print("---")
