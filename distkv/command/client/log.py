# command line interface

import os
import sys
import asyncclick as click
import json

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

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage logging.")
@click.pass_obj
async def cli(obj):
    """
    This subcommand controls a server's logging.
    """
    pass


@cli.command()
@click.option("-i", "--incremental", is_flag=True, help="Don't write the initial state")
@click.argument("path", nargs=1)
@click.pass_obj
async def dest(obj, path, incremental):
    """
    Log changes to a file.

    Any previously open log (on the server you talk to) is closed as soon
    as the new one is opened and ready.
    """
    res = await obj.client._request("log", path=path, fetch=not incremental)
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command()
@click.argument("path", nargs=1)
@click.pass_obj
async def save(obj, path):
    """
    Write the server's current state to a file.
    """
    res = await obj.client._request("save", path=path)
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command()
@click.pass_obj
async def stop(obj):
    """
    Stop logging changes.
    """
    res = await obj.client._request("log")  # no path == stop
    if obj.meta:
        yprint(res, stream=obj.stdout)
