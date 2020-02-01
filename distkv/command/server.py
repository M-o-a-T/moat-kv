# command line interface

import os
import asyncclick as click
import anyio

from distkv.default import CFG
from distkv.server import Server

import logging

logger = logging.getLogger(__name__)


@main.command(short_help="Run the DistKV server.")  # pylint: disable=undefined-variable
@click.option(
    "-l",
    "--load",
    type=click.Path(readable=True, exists=True, allow_dash=False),
    default=None,
    help="Event log to preload.",
)
@click.option(
    "-s",
    "--save",
    type=click.Path(writable=True, allow_dash=False),
    default=None,
    help="Event log to write to.",
    hidden=True,
)
@click.option(
    "-i",
    "--incremental",
    default=None,
    help="Save incremental changes, not the complete state",
    hidden=True,
)
@click.option(
    "-I",
    "--init",
    default=None,
    help="Initial value to set the root to. Use only when setting up "
    "a cluster for the first time!",
    hidden=True,
)
@click.option("-e", "--eval", is_flag=True, help="The 'init' value shall be evaluated.", hidden=True)
@click.argument("name", nargs=1)
@click.pass_obj
async def cli(obj, name, load, save, init, incremental, eval):
    """
    This command starts a DistKV server. It defaults to connecting to the local Serf
    agent.

    All DistKV servers must have a unique name. Its uniqueness cannot be
    verified reliably.

    One server in your network needs either an initial datum, or a copy of
    a previously-saved DistKV state. Otherwise, no client connections will
    be accepted until synchronization with the other servers in your DistKV
    network is complete.

    This command requires a unique NAME argument. The name identifies this
    server on the network. Never start two servers with the same name!
    """

    kw = {}
    if eval:
        kw["init"] = __builtins__["eval"](init)
    elif init == "-":
        kw["init"] = None
    elif init is not None:
        kw["init"] = init

    from distkv.util import as_service

    async with as_service(obj) as evt:
        s = Server(name, cfg=obj.cfg, **kw)
        if load is not None:
            await s.load(path=load, local=True)

        await s.serve(log_path=save, log_inc=incremental, ready_evt=evt)
