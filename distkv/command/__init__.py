# command line interface

import sys

import asyncclick as click
from moat.util import attrdict, main_

from distkv.default import CFG
from distkv.exceptions import ClientError, ServerError


def cmd(backend="trio"):
    """
    The main command entry point, as declared in ``setup.py``.
    """
    click.anyio_backend = "trio"

    # @click.* decorators change the semantics
    # pylint: disable=no-value-for-parameter
    main_.help = """\
This is DistKV, a distributed master-less key-value storage system.
"""
    obj = attrdict(
        moat=attrdict(
            ext_pre="distkv_ext",
            name="distkv",
            sub_pre="distkv.command",
            sub_post="cli",
            ext_post="main.cli",
            CFG=CFG,
        )
    )
    try:
        main_(obj=obj, _anyio_backend=backend)
    except (ClientError, ServerError) as err:
        print(type(err).__name__ + ":", *err.args, file=sys.stderr)
        sys.exit(1)


@main_.command(
    short_help="Import the debugger", help="Imports PDB and then continues to process arguments."
)
@click.argument("args", nargs=-1)
async def pdb(args):  # safe
    breakpoint()  # pylint: disable=forgotten-debug-statement
    if not args:
        return
    return await main_.main(args)
