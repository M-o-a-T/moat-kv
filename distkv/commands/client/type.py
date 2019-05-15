# command line interface

import os
import sys
import trio_click as click
from pprint import pprint
import json

from distkv.util import (
    attrdict,
    combine_dict,
    PathLongener,
    MsgReader,
    PathShortener,
    split_one,
    NotGiven,
)
from distkv.client import open_client, StreamedRequest
from distkv.command import Loader
from distkv.default import CFG
from distkv.server import Server
from distkv.auth import loader, gen_auth
from distkv.exceptions import ClientError

import logging

logger = logging.getLogger(__name__)


@main.group()
@click.pass_obj
async def cli(obj):
    """Manage types and type matches. Usage: … type …"""
    pass


@cli.command()
@click.option("-y", "--yaml", is_flag=True, help="Print as YAML. Default: Python.")
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-s", "--script", type=click.File(mode="w", lazy=True), help="Save the script here"
)
@click.option(
    "-S", "--schema", type=click.File(mode="w", lazy=True), help="Save the schema here"
)
@click.argument("path", nargs=-1)
@click.pass_obj
async def get(obj, path, chain, yaml, verbose, script, schema):
    """Read type checker information"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal",
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if not verbose:
        res = res.value
    if yaml:
        import yaml

        if schema and res.get("schema", None) is not None:
            print(
                yaml.safe_dump(res.pop("schema"), default_flow_style=False), file=schema
            )
        print(yaml.safe_dump(res, default_flow_style=False), file=script or sys.stdout)
    else:
        if script:
            code = res.pop("code", None)
            if code is not None:
                print(code, file=script)
        if schema and res.get("schema", None) is not None:
            json.dump(res.pop("schema"), schema)
        pprint(res)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option("-g", "--good", multiple=True, help="Example for passing values")
@click.option("-b", "--bad", multiple=True, help="Example for failing values")
@click.option(
    "-s", "--script", type=click.File(mode="r"), help="File with the checking script"
)
@click.option(
    "-S", "--schema", type=click.File(mode="r"), help="File with the JSON schema"
)
@click.option("-y", "--yaml", is_flag=True, help="load everything from this file")
@click.argument("path", nargs=-1)
@click.pass_obj
async def set(obj, path, good, bad, verbose, script, schema, yaml):
    """Write type checker information."""
    if not path:
        raise click.UsageError("You need a non-empty path.")

    if yaml:
        import yaml

        msg = yaml.safe_load(script)
    else:
        msg = {}
    msg.setdefault("good", [])
    msg.setdefault("bad", [])
    for x in good:
        msg["good"].append(eval(x))
    for x in bad:
        msg["bad"].append(eval(x))
    if "code" not in msg:
        if not script:
            if os.isatty(sys.stdin.fileno()):
                print("Enter the Python script to verify 'value'.")
            script = sys.stdin.read()
        else:
            script = script.read()
        msg["code"] = script
    elif script and not yaml:
        raise click.UsageError("Duplicate script parameter")
    if "schema" not in msg:
        if schema:
            if yaml:
                schema = yaml.safe_load(schema)
            else:
                schema = json.load(schema)
            msg["schema"] = schema
    elif schema:
        raise click.UsageError("Duplicate schema parameter")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("type",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if verbose:
        print(res.tock)


@cli.command()
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Print the complete result. Default: just the value",
)
@click.option(
    "-t", "--type", multiple=True, help="Type to link to. Multiple for subytpes."
)
@click.option("-d", "--delete", help="Use to delete this mapping.")
@click.argument("path", nargs=-1)
@click.pass_obj
async def match(obj, path, type, delete, verbose):
    """Match a type to a path (read, if no type given)"""
    if not path:
        raise click.UsageError("You need a non-empty path.")
    if type and delete:
        raise click.UsageError("You can't both set and delete a path.")

    if delete:
        await obj.client._request(action="delete_internal", path=("type",) + path)
        return

    msg = {"type": type}
    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=("match",) + path,
        iter=False,
        nchain=3 if verbose else 0,
    )
    if type or delete:
        print(res.tock)
    elif verbose:
        pprint(res)
    else:
        print(" ".join(res.type))


