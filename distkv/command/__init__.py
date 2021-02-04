# command line interface

import os
import sys
import asyncclick as click
from functools import partial

from distkv.util import (
    attrdict,
    combine_dict,
    NotGiven,
    yload,
    yprint,
    call_main,
    main,
)
from distkv.default import CFG
from distkv.exceptions import ClientError, ServerError
from distkv.data import res_get, res_update, res_delete
from distkv.util import Loader

import logging
logger = logging.getLogger(__name__)


def cmd():
    """
    The main command entry point, as declared in ``setup.py``.
    """
    try:
        # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
        call_main(main, name="distkv", sub=True)
    except (ClientError, ServerError) as err:
        print(type(err).__name__ + ":", *err.args, file=sys.stderr)
        sys.exit(1)


@main.command(
    short_help="Import the debugger", help="Imports PDB and then continues to process arguments."
)
@click.argument("args", nargs=-1)
async def pdb(args):  # safe
    breakpoint()  # safe
    if not args:
        return
    return await main.main(args)
