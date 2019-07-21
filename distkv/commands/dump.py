# command line interface

import os
import sys
import asyncclick as click
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
@click.argument("path", nargs=-1)
@click.pass_obj
async def cfg(obj, path):
    """Emit the current configuration as a YAML file.
    
    You can limit the output by path elements.
    E.g., "cfg connect host" will print "localhost".

    Single values are printed with a trailing line feed.
    """
    cfg = obj.cfg
    for p in path:
        try:
            cfg = cfg[p]
        except KeyError:
            if obj.debug:
                print("Unknown:",p)
            sys.exit(1)
    if isinstance(cfg,str):
        print(cfg, file=obj.stdout)
    else:
        yprint(cfg, stream=obj.stdout)


@cli.command()
@click.argument("file", nargs=1)
@click.pass_obj
async def file(obj, file):
    """Read a MsgPack file and dump as YAML."""
    async with MsgReader(path=file) as f:
        async for msg in f:
            yprint(msg, stream=obj.stdout)
            print("---", file=obj.stdout)
