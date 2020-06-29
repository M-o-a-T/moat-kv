# command line interface

import asyncclick as click

from distkv.util import yprint, NotGiven, PathLongener, yload, P, Path

import logging

logger = logging.getLogger(__name__)


@main.group()  # pylint: disable=undefined-variable
async def cli():
    """Manage codecs and converters. Usage: … codec …"""
    pass


@cli.command()
@click.option("-e", "--encode", type=click.File(mode="w", lazy=True), help="Save the encoder here")
@click.option("-d", "--decode", type=click.File(mode="w", lazy=True), help="Save the decoder here")
@click.option("-s", "--script", type=click.File(mode="w", lazy=True), help="Save the data here")
@click.argument("path", nargs=1)
@click.pass_obj
async def get(obj, path, script, encode, decode):
    """Read type information"""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")
    res = await obj.client._request(
        action="get_internal", path=Path("codec") + path, iter=False, nchain=obj.meta
    )
    if encode and res.get("encode", None) is not None:
        encode.write(res.pop("encode"))
    if decode and res.get("decode", None) is not None:
        decode.write(res.pop("decode"))

    if not obj.meta:
        res = res.value
    yprint(res, stream=script or obj.stdout)


@cli.command(name="list")
@click.pass_obj
@click.argument("path", nargs=1)
async def list_(obj, path):
    """List type information entries"""
    res = await obj.client._request(
        action="get_tree_internal", path=Path("codec") + path, iter=True, nchain=obj.meta
    )
    pl = PathLongener(())
    async for r in res:
        pl(r)
        print(" ".join(str(x) for x in r.path), file=obj.stdout)


@cli.command("set")
@click.option("-e", "--encode", type=click.File(mode="r"), help="File with the encoder")
@click.option("-d", "--decode", type=click.File(mode="r"), help="File with the decoder")
@click.option("-D", "--data", type=click.File(mode="r"), help="File with the rest")
@click.option("-i", "--in", "in_", nargs=2, multiple=True, help="Decoding sample")
@click.option("-o", "--out", nargs=2, multiple=True, help="Encoding sample")
@click.argument("path", nargs=1)
@click.pass_obj
async def set_(obj, path, encode, decode, data, in_, out):
    """Save codec information"""
    path = P(path)
    if not len(path):
        raise click.UsageError("You need a non-empty path.")

    if data:
        msg = yload(data)
    else:
        msg = {}
    chain = NotGiven
    if "value" in msg:
        chain = msg.get("chain", NotGiven)
        msg = msg["value"]

    if "encode" in msg:
        if encode:
            raise click.UsageError("Duplicate encode script")
    else:
        if not encode:
            raise click.UsageError("Missing encode script")
        msg["encode"] = encode.read()
    if "decode" in msg:
        if decode:
            raise click.UsageError("Duplicate decode script")
    else:
        if not decode:
            raise click.UsageError("Missing decode script")
        msg["decode"] = decode.read()
    if in_:
        msg["in"] = [(eval(a), eval(b)) for a, b in in_]  # pylint: disable=eval-used
    if out:
        msg["out"] = [(eval(a), eval(b)) for a, b in out]  # pylint: disable=eval-used

    if not msg["in"]:
        raise click.UsageError("Missing decode tests")
    if not msg["out"]:
        raise click.UsageError("Missing encode tests")

    res = await obj.client._request(
        action="set_internal",
        value=msg,
        path=Path("codec") + path,
        iter=False,
        nchain=obj.meta,
        **({} if chain is NotGiven else {"chain": chain}),
    )
    if obj.meta:
        yprint(res, stream=obj.stdout)


@cli.command()
@click.option("-c", "--codec", help="Codec to link to. Multiple for hierarchical.")
@click.option("-d", "--delete", is_flag=True, help="Use to delete this converter.")
@click.option(
    "-l", "--list", "list_this", is_flag=True, help="Use to list this converter; '-' to list all."
)
@click.argument("name", nargs=1)
@click.argument("path", nargs=1)
@click.pass_obj
async def convert(obj, path, codec, name, delete, list_this):
    """Match a codec to a path (read, if no codec given)"""
    path = P(path)
    if delete and list_this:
        raise click.UsageError("You can't both list and delete a path.")
    if not len(path) and not list_this:
        raise click.UsageError("You need a non-empty path.")
    if codec and delete:
        raise click.UsageError("You can't both set and delete a path.")

    if list_this:
        if name == "-":
            if path:
                raise click.UsageError("You can't use a path here.")
            res = await obj.client._request(
                action="enum_internal", path=Path("conv"), iter=False, nchain=0, empty=True
            )
            for r in res.result:
                print(r, file=obj.stdout)

        else:
            res = await obj.client._request(
                action="get_tree_internal",
                path=Path("conv", name) + path,
                iter=True,
                nchain=obj.meta,
            )
            pl = PathLongener(())
            async for r in res:
                pl(r)
                try:
                    print(
                        " ".join(str(x) for x in r.path),
                        ":",
                        " ".join(r.value["codec"]),
                        file=obj.stdout,
                    )
                except Exception as e:
                    print(" ".join(str(x) for x in r.path), ":", e)

        return
    if delete:
        res = await obj.client._request(action="delete_internal", path=Path("conv", name) + path)
    else:
        msg = {"codec": P(codec)}
        res = await obj.client._request(
            action="set_internal",
            value=msg,
            path=Path("conv", name) + path,
            iter=False,
            nchain=obj.meta,
        )
    if obj.meta:
        yprint(res, stream=obj.stdout)
    elif type or delete:
        print(res.tock, file=obj.stdout)
    else:
        print(" ".join(res.type), file=obj.stdout)
