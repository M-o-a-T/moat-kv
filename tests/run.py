#from trio_click.testing import CliRunner
import io
import sys

from distkv.command import main

async def run(*args, expect_exit=0):
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        res = await main.main(args)
        return res
    except SystemExit as exc:
        res = exc
        assert exc.code == expect_exit, exc.code
        return exc
    except BaseException as exc:
        res = exc
        raise
    else:
        assert expect_exit == 0
        return res
    finally:
        res.stdout = sys.stdout.getvalue()
        sys.stdout = old_stdout
