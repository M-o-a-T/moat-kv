# command line interface

import os
import sys
import asyncclick as click
from functools import partial

from distkv.util import (
    attrdict,
    combine_dict,
    NotGiven,
    res_get,
    res_update,
    res_delete,
    yload,
    yprint,
)
from distkv.default import CFG
from distkv.ext import load_one, list_ext, load_ext
from distkv.exceptions import ClientError, ServerError

import logging
from logging.config import dictConfig

logger = logging.getLogger(__name__)


class Loader(click.Group):
    """
    A Group that can load additional commands from a subfolder.

    Caller:

        from distkv.command import Loader
        from functools import partial

        @click.command(cls=partial(Loader,__file__,'command'))
        async def cmd()
            print("I am the main program")

    Sub-Command Usage (``main`` is defined for you), e.g. in ``command/subcmd.py``::

        from distkv.command import Loader
        from functools import partial

        @main.command / group()
        async def cmd(self):
            print("I am", self.name)  # prints "subcmd"
    """

    def __init__(self, current_file, plugin, *a, **kw):
        self.__plugin = plugin
        self.__plugin_folder = os.path.dirname(current_file)
        super().__init__(*a, **kw)

    def list_commands(self, ctx):
        rv = super().list_commands(ctx)

        for filename in os.listdir(self.__plugin_folder):
            if filename[0] in "._":
                continue
            if filename.endswith(".py"):
                rv.append(filename[:-3])
            elif os.path.isfile(os.path.join(self.__plugin_folder, filename, "__init__.py")):
                rv.append(filename)

        for n, _ in list_ext(self.__plugin):
            rv.append(n)
        rv.sort()
        return rv

    def get_command(self, ctx, name):  # pylint: disable=arguments-differ
        command = super().get_command(ctx, name)
        if command is None:
            try:
                command = load_one(name, self.__plugin_folder, "cli", main=self)
            except FileNotFoundError:
                command = load_ext(name, self.__plugin, "cli", main=self)
        command.__name__ = name
        return command


async def node_attr(
    obj, path, attr, value=NotGiven, eval_=False, split_=False, res=None, chain=None
):
    """
    Sub-attr setter.

    Args:
        obj: command object
        path: address of the node to change
        attr: path of the element to change
        value: new value (default NotGiven)
        eval_: evaluate the new value? (default False)
        split_: split a string value into words? (bool or separator, default False)
        res: old node, if it has been read already
        chain: change chain of node, copied from res if clear

    Special: if eval_ is True, a value of NotGiven deletes, otherwise it
    prints the record without changing it. A mapping replaces instead of updating.

    Returns the result of setting the attribute, or ``None`` if it printed
    """
    if res is None:
        res = await obj.client.get(path, nchain=obj.meta or 2)
    if chain is None:
        chain = res.chain

    try:
        val = res.value
    except AttributeError:
        chain = None
    if split_ is True:
        split_ = ""
    if eval_:
        if value is NotGiven:
            value = res_delete(res, attr)
        else:
            value = eval(value)  # pylint: disable=eval-used
            if split_ is not False:
                value = value.split(split_)
            value = res_update(res, attr, value=value)
    else:
        if value is NotGiven:
            if not attr and obj.meta:
                val = res
            else:
                val = res_get(res, attr)
            yprint(val, stream=obj.stdout)
            return
        if split_ is not False:
            value = value.split(split_)
        value = res_update(res, attr, value=value)

    res = await obj.client.set(path, value=value, nchain=obj.meta, chain=chain)
    return res


def cmd():
    """
    The main command entry point, as declared in ``setup.py``.
    """
    try:
        # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
        main(standalone_mode=False)
    except click.exceptions.MissingParameter as exc:
        print(f"You need to provide an argument { exc.param.name.upper() !r}.\n", file=sys.stderr)
        print(exc.cmd.get_help(exc.ctx), file=sys.stderr)
        sys.exit(2)
    except click.exceptions.UsageError as exc:
        try:
            s = str(exc)
        except TypeError:
            logger.exception(repr(exc), exc_info=exc)
        else:
            print(s, file=sys.stderr)
        sys.exit(2)
    except click.exceptions.Abort:
        print("Aborted.", file=sys.stderr)
        pass
    except EnvironmentError:  # pylint: disable=try-except-raise
        raise
    except (ClientError, ServerError) as err:
        print(type(err).__name__ + ":", *err.args, file=sys.stderr)
        sys.exit(1)


#   except BaseException as exc:
#       print(exc)
#       sys.exit(1)


@click.command(cls=partial(Loader, __file__, "command"))
@click.option(
    "-v", "--verbose", count=True, help="Enable debugging. Use twice for more verbosity."
)
@click.option(
    "-l", "--log", multiple=True, help="Adjust log level. Example: '--log asyncactor=DEBUG'."
)
@click.option("-q", "--quiet", count=True, help="Disable debugging. Opposite of '--verbose'.")
@click.option("-D", "--debug", is_flag=True, help="Enable debug speed-ups (smaller keys etc).")
@click.option("-c", "--cfg", type=click.File("r"), default=None, help="Configuration file (YAML).")
@click.option(
    "-C",
    "--conf",
    multiple=True,
    help="Override a config entry. Example: '-C server.bind_default.port=57586'",
)
@click.pass_context
async def main(ctx, verbose, quiet, debug, log, cfg, conf):
    """
    This is the DistKV command. You need to add a subcommand for it to do
    anything.

    You can print the current configuration with 'distkv dump cfg'.
    """
    ctx.ensure_object(attrdict)
    ctx.obj.debug = max(verbose - quiet + 1, 0)
    ctx.obj._DEBUG = debug
    ctx.obj.stdout = CFG.get("_stdout", sys.stdout)  # used for testing

    def _cfg(path):
        nonlocal cfg
        if cfg is not None:
            return
        if os.path.exists(path):
            try:
                cfg = open(path, "r")
            except PermissionError:
                pass

    _cfg(os.path.expanduser("~/config/distkv.cfg"))
    _cfg(os.path.expanduser("~/.config/distkv.cfg"))
    _cfg(os.path.expanduser("~/.distkv.cfg"))
    _cfg("/etc/distkv/distkv.cfg")
    _cfg("/etc/distkv.cfg")

    if cfg:
        logger.debug("Loading %s", cfg)

        cd = yload(cfg)
        if cd is None:
            ctx.obj.cfg = CFG
        else:
            ctx.obj.cfg = combine_dict(cd, CFG, cls=attrdict)
        cfg.close()
    else:
        ctx.obj.cfg = CFG

    # One-Shot-Hack the config file.
    for k in conf:
        try:
            k, v = k.split("=", 1)
        except ValueError:
            v = NotGiven
        else:
            try:
                v = eval(v)  # pylint: disable=eval-used
            except Exception:  # pylint: disable=broad-except
                pass
        c = ctx.obj.cfg
        *sl, s = k.split(".")
        for kk in sl:
            try:
                c = c[kk]
            except KeyError:
                c[kk] = attrdict()
                c = c[kk]
        if v is NotGiven:
            del c[s]
        else:
            c[s] = v

    # Configure logging. This is a somewhat arcane art.
    lcfg = ctx.obj.cfg.logging
    lcfg["root"]["level"] = (
        "DEBUG" if verbose > 2 else "INFO" if verbose > 1 else "WARNING" if verbose else "ERROR"
    )
    for k in log:
        k, v = k.split("=")
        lcfg["loggers"].setdefault(k, {})["level"] = v
    dictConfig(lcfg)
    logging.captureWarnings(verbose > 0)


@main.command(
    short_help="Import the debugger", help="Imports PDB and then continues to process arguments."
)
@click.argument("args", nargs=-1)
async def pdb(args):  # safe
    import pdb  # pylint: disable=redefined-outer-name

    pdb.set_trace()  # safe
    if not args:
        return
    return await main.main(args)
