"""
This module implements additional code for the "core" Actors, which is used
to clean up the list of deleted nodes, resp. their chain links.
"""

from collections import deque

from asyncserf.actor import Actor

TAGS = 4


class CoreActor:
    def __init__(self, server):
        self.server = server
        self.deleted = deque()
        self.tags = []
        self.actor = None

    def tagged(self):
        self.tags.append(self.server.tock)
        self.tags = self.tags[-TAGS:]
        return self.tags[0], self.tags[-1]

    def add_deleted(self, add):
        self.deleted.append((self.server.tock, add.copy()))

    def purge_to(self, tock):
        while self.deleted and self.deleted[0][0] < tock:
            self.deleted.popleft()

    def history_pos(self, name):
        return self.actor.history_pos(name)

    async def enable(self, length):
        self.actor.enable(length)
        await self.actor.enable()

    async def disable(self):
        await self.actor.disable()

    def run(self, enable=False, evt=None):
        try:
            cfg = self.server.cfg.get('core', {})
            async with Actor(self.server, prefix=self.server.cfg["root"] + ".core",
                    name=self.server.node.name, cfg=cfg, enable=enable) as actor:
                self.actor = actor
                if evt is not None:
                    evt.set()
                async for evt in actor:
                    if isinstance(evt, TagEvent):
                        if actor.history_size == len(client.core_nodes):
                            await self.self.tagged()
        finally:
            self.actor = None
