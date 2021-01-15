#!/usr/bin/python3

# Batch-convert data. In this case I had some cable links from before these were
# stored as Path, and needed to edit the raw data. That's much easier when
# they're not a list.

import anyio
from distkv.client import open_client
from distkv.util import P, yload, Path

def conv(m,s: str) -> bool:
    d = m.value[s]
    if isinstance(d[0],Path):
        return True
    d = (Path.build(d[0]), *d[1:])
    m.value[s] = d
    return False

ORIG=P(":.distkv.inventory.cable")

async def dkv_example():
    with open("/etc/distkv.cfg") as cff:
        cfg = yload(cff)
    async with open_client(**cfg) as client:
        async for m in client.get_tree(ORIG, nchain=2):
            if conv(m,'a') + conv(m,'b') == 2:
                continue
            print(m)
            await client.set(ORIG+m.path, value=m.value, chain=m.chain)

anyio.run(dkv_example)

