# command line interface

import datetime
import sys
from collections.abc import Mapping

import asyncclick as click
from distmqtt.codecs import MsgPackCodec
from moat.util import (
    MsgReader,
    MsgWriter,
    P,
    Path,
    PathLongener,
    load_subgroup,
    yload,
    yprint,
)

from distkv.codec import unpacker


@load_subgroup(short_help="Manage data.", plugin="dump")  # pylint: disable=undefined-variable
async def cli():
    """
    Low-level tools that don't depend on a running DistKV server.
    """
    pass


@cli.command("cfg")
@click.argument("path", nargs=1)
@click.pass_obj
async def cfg_dump(obj, path):
    """Emit the current configuration as a YAML file.

    You can limit the output by path elements.
    E.g., "cfg connect.host" will print "localhost".

    Single values are printed with a trailing line feed.

    Dump the whole config with "… dump cfg :".
    """
    cfg = obj.cfg
    for p in P(path):
        try:
            cfg = cfg[p]
        except KeyError:
            if obj.debug:
                print("Unknown:", p)
            sys.exit(1)
    if isinstance(cfg, str):
        print(cfg, file=obj.stdout)
    else:
        yprint(cfg, stream=obj.stdout)


@cli.command("file")
@click.option("-p", "--path", is_flag=True, default=False, help="Unwrap paths")
@click.option("-f", "--filter", "filter_", multiple=True, help="Only emit entries with this path")
@click.argument("file", nargs=1)
@click.pass_obj
async def file_(obj, file, path, filter_):
    """Read a MsgPack file and dump as YAML."""
    if path or filter_:
        pl = PathLongener()
    else:
        pl = lambda _: None
    filter_ = [P(x) for x in filter_]
    async with MsgReader(path=file) as f:
        async for msg in f:
            pl(msg)
            if filter_:
                if "path" not in msg:
                    continue
                for f in filter_:
                    if msg.path[: len(f)] == f:
                        break
                else:
                    continue
            yprint(msg, stream=obj.stdout)
            print("---", file=obj.stdout)


@cli.command("yaml")
@click.argument("msgpack", nargs=1)
async def yaml_(msgpack):
    """Read a multi-part YAML file from stdin and dump as msgpack stream."""
    async with MsgWriter(path=msgpack) as f:
        for d in yload(sys.stdin, multi=True):
            await f(d)


@cli.command()
@click.argument("node", nargs=1)
@click.argument("file", type=click.Path(), nargs=1)
async def init(node, file):
    """Write an initial preload file.

    Usage: distkv dump init <node> <outfile>

    Writes an initial DistKV file that behaves as if it was generated by <node>.

    Using this command, followed by "distkv server -l <outfile> <node>", is
    equivalent to running "distkv server -i 'Initial data' <node>.
    """
    async with MsgWriter(path=file) as f:
        await f(
            dict(
                chain=dict(node=node, tick=1, prev=None),
                depth=0,
                path=[],
                tock=1,
                value="Initial data",
            )
        )


@cli.command("msg")
@click.argument("path", nargs=1)
@click.pass_obj
async def msg_(obj, path):
    """
    Monitor the server-to-sever message stream.

    Use ':' for the main server's "update" stream.
    Use '+NAME' to monitor a different stream instead.
    Use '+' to monitor all streams.
    Otherwise use the given name as-is; Mosquitto wildcard rules apply.

    \b
    Common streams (prefix with '+'):
    * ping    all servers
    * update  data changes
    * del     nodes responsible for cleaning up deleted records
    """
    from distkv.backend import get_backend

    class _Unpack:
        def __init__(self):
            self._part_cache = dict()

    import distkv.server

    _Unpack._unpack_multiple = distkv.server.Server._unpack_multiple
    _unpacker = _Unpack()._unpack_multiple

    path = P(path)
    if len(path) == 0:
        path = P(obj.cfg.server.root) | "update"
    elif len(path) == 1 and path[0].startswith("+"):
        p = path[0][1:]
        path = P(obj.cfg.server.root)
        path |= p or "#"
    be = obj.cfg.server.backend
    kw = obj.cfg.server[be]

    async with get_backend(be)(**kw) as conn:
        async with conn.monitor(*path) as stream:
            async for msg in stream:
                v = vars(msg)
                if isinstance(v.get("payload"), (bytearray, bytes)):
                    t = msg.topic
                    v = unpacker(v["payload"])
                    v = _unpacker(v)
                    if v is None:
                        continue
                    if not isinstance(v, Mapping):
                        v = {"_data": v}
                    v["_topic"] = Path.build(t)
                else:
                    v["_type"] = type(msg).__name__

                v["_timestamp"] = datetime.datetime.now().isoformat(
                    sep=" ", timespec="milliseconds"
                )

                yprint(v, stream=obj.stdout)
                print("---", file=obj.stdout)


@cli.command("post")
@click.argument("path", nargs=1)
@click.pass_obj
async def post_(obj, path):
    """
    Send a msgpack-encoded message (or several) to a specific MQTT topic.

    Messages are read from YAML.
    Common streams:
    * ping: sync: all servers (default)
    * update: data changes
    * del: sync: nodes responsible for cleaning up deleted records
    """
    from distkv.backend import get_backend

    path = P(path)
    be = obj.cfg.server.backend
    kw = obj.cfg.server[be]

    async with get_backend(be)(codec=MsgPackCodec, **kw) as conn:
        for d in yload(sys.stdin, multi=True):
            topic = d.pop("_topic", path)
            await conn.send(*topic, payload=d)
