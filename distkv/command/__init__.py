# command line interface

import sys
import asyncclick as click

from distkv.util import main_
from distkv.exceptions import ClientError, ServerError

import logging

logger = logging.getLogger(__name__)


def cmd():
    """
    The main command entry point, as declared in ``setup.py``.
    """
    try:
        # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
        main_()
    except (ClientError, ServerError) as err:
        print(type(err).__name__ + ":", *err.args, file=sys.stderr)
        sys.exit(1)


@main_.command(
    short_help="Import the debugger", help="Imports PDB and then continues to process arguments."
)
@click.argument("args", nargs=-1)
async def pdb(args):  # safe
    breakpoint()  # safe
    if not args:
        return
    return await main.main(args)
