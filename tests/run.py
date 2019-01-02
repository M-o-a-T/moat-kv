#from trio_click.testing import CliRunner
from distkv.command import main

async def run(*args, expect_exit=0):
    try:
        res = await main.main(args)
    except SystemExit as exc:
        assert exc.code == expect_exit, exc.code
    else:
        assert expect_exit == 0
