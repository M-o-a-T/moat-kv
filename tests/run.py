# from asyncclick.testing import CliRunner
import io
import sys

from distkv.command import main
from distkv.default import CFG


async def run(*args, expect_exit=0, do_stdout=True):
    args = ["-c","/dev/null", *args]
    if do_stdout:
        CFG["_stdout"] = out = io.StringIO()
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
        if do_stdout:
            res.stdout = out.getvalue()
            CFG["_stdout"] = sys.stdout
