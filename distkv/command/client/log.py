# command line interface

import asyncclick as click

from distkv.util import yprint

import logging

logger = logging.getLogger(__name__)


@main.group(short_help="Manage logging.")  # pylint: disable=undefined-variable
async def cli():
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
@click.option("-f", "--full", is_flag=1, help="Also dump internal state")
@click.argument("path", nargs=1)
@click.pass_obj
async def save(obj, path, full):
    """
    Write the server's current state to a file.
    """
    res = await obj.client._request("save", path=path, full=full)
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
