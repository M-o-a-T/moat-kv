# command line interface

import sys
import asyncclick as click

from distkv.util import main_
from distkv.exceptions import ClientError, ServerError


def cmd():
    """
    The main command entry point, as declared in ``setup.py``.
    """
    click.anyio_backend = "trio"

    try:
        # @click.* decorators change the semantics
        # pylint: disable=no-value-for-parameter
        main_.help = """\
This is DistKV, a distributed master-less key-value storage system.
"""
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
    return await main_.main(args)
