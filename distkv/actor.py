"""
This module implements a :cls:`asyncserf.actor.Actor` which works on top of
a DistKV client.
"""

import anyio
from asyncserf.actor import Actor


class ClientActor(Actor):
    async def read_task(self, prefix: str, evt: anyio.abc.Event = None):
        async with self._client.serf_mon(prefix) as mon:      
            self.logger.debug("start listening")
            await evt.set()
            async for msg in mon:
                self.logger.debug("recv %r", msg.data)
                await self._rdr_q.put(msg.data)


    async def send_event(self, prefix, msg):
        await self._client.serf_send(prefix, msg)

