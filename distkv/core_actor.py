"""
This module implements additional code for the "core" Actors, which is used
to clean up the list of deleted nodes.
"""

import anyio
import weakref
from collections import deque

from asyncserf.actor import Actor
from asyncserf.actor import PingEvent, TagEvent

from .codec import packer, unpacker


TAGS = 4


class CoreActor:
    def __init__(self, server):
        self._server = weakref.ref(server)
        self.deleted = deque()
        self.tags = []
        self.actor = None

        self.max_seen = 0
        self.n_tags = 0
        self.n_pings = 0
        self.n_nodes = 0

    @property
    def server(self):
        return self._server()

    async def tock_me(self):
        """
        Add the current tock to our buffer.

        This is updated whenever a new leader is selected.
        """
        self.tags.append(self.server.tock)
        self.tags = self.tags[-TAGS:]
        await self.actor.set_value((self.tags[0], self.tags[-1]))

    def add_deleted(self, nodes: "NodeSet"):
        """
        These nodes are deleted. Remember them for some time.
        """
        if self.n_nodes == 0:
            return
        self.deleted.append((self.server.tock, nodes))

    def purge_to(self, tock):
        """
        Sufficient time has passed since this tock was seen, while all core
        nodes were active. Finally flush the entries that have been deleted
        before it.
        """
        while self.deleted and self.deleted[0][0] < tock:
            d = self.deleted.popleft()
            self.server.purge_deleted(d[1])

    async def enable(self, n):
        """
        Enable this actor, as a group of N.
        """
        await self.actor.enable(n)
        self.n_tags = 0
        self.n_pings = 0
        self.n_nodes = n

    async def disable(self, n: int = 0):
        """
        Disable this actor. It will still listen, and require N core
        members in order to flush its deletion entries.

        Completely disable deletion flushing by passing n=0.
        """
        await self.actor.disable()
        self.n_tags = 0
        self.n_pings = 0
        self.n_nodes = n

    async def run(self, evt=None):
        """
        The task that monitors the core actor.
        """
        try:
            cfg = self.server.cfg.get("core", {})
            async with anyio.create_task_group() as tg:
                async with Actor(
                    self.server.serf,
                    prefix=self.server.cfg["root"] + ".core",
                    name=self.server.node.name,
                    cfg=cfg,
                    tg=tg,
                    enabled=False,
                    packer=packer,
                    unpacker=unpacker,
                ) as actor:
                    self.actor = actor
                    if evt is not None:
                        evt.set()
                    async for evt in actor:
                        if isinstance(evt, PingEvent):
                            val = evt.value
                            if val is None:
                                self.n_pings = self.n_tags = 0
                                continue
                            if len(evt.msg['history']) < self.n_nodes:
                                self.n_pings = self.n_tags = 0
                                continue
                            self.n_pings += 1
                            if self.n_pings > self.n_nodes:
                                mx,self.max_seen = self.max_seen,max(self.max_seen, val[1])
                                if val[0] > mx > 0:
                                    await self.server.resync_deleted(evt.msg['history'])
                                    continue
                                self.purge_to(val[0])
                                self.max_seen = max(self.max_seen, val[1])

                        elif isinstance(evt, TagEvent):
                            if actor.history_size == self.n_nodes:
                                self.n_tags += 1
                                if self.n_tags > 2:
                                    self.purge_to(self.tags[0])
                                await self.tock_me()
        finally:
            self.actor = None
