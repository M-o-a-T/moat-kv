#!/usr/bin/python3

# Batch-convert data. In this case I had some entries which were stored as
# a list, but using Path made much more sense (esp when you need to
# view/edit the yaml export).

import anyio
from moat.kv.client import open_client
from moat.util import P, yload, Path
import asyncclick as click

def conv(m,s: str) -> bool:
    try:
        d = m.value[s]
    except KeyError:
        return 0
    if isinstance(d,Path):
        return 0
    if not isinstance(d,Sequence):
        return 0
    d = Path.build(d)
    m.value[s] = d
    return 1

@click.command()
@click.argument("path", type=P)
@click.argument("keys", type=str, nargs=-1)
async def main(path, keys):
    if not keys:
        keys = "src dest dst state".split()
    with open("/etc/moat.kv.cfg") as cff:
        cfg = yload(cff)
    async with open_client(**cfg) as client:
        async for m in client.get_tree(path, nchain=2):
            n = 0
            for k in keys:
                n += conv(m,k)
            if n:
                await client.set(ORIG+m.path, value=m.value, chain=m.chain)

if __name__ == "__main__":
    main()

