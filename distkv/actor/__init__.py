"""
This module implements a :cls:`asyncserf.actor.Actor` which works on top of
a DistKV client.
"""

import anyio
from asyncserf.actor import Actor
from ..codec import packer, unpacker


class ClientActor(Actor):
    """
    This is an Actor which works on top of a DistKV client.
    """
    def __init__(self, client, name, prefix=None, cfg=None, tg=None):
        if prefix is None:
            prefix=".".join(cfg["prefix"][1:])
        if cfg is None:
            cfg = {}
        super().__init__(
            client, name=name, prefix=prefix, cfg=cfg.get("actor", {}),
            packer=packer, unpacker=unpacker, tg=tg,
        )

    async def read_task(self, prefix: str, evt: anyio.abc.Event = None):
        async with self._client.serf_mon(prefix) as mon:      
            await evt.set()
            async for msg in mon:
                await self.queue_msg(msg.data)


    async def send_event(self, prefix, msg):
        await self._client.serf_send(prefix, msg)

