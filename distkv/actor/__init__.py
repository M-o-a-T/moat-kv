"""
This module implements a :class:`asyncactor.Actor` which works on top of
a DistKV client.
"""

import anyio
from ..util import singleton
from asyncactor.abc import Transport, MonitorStream
from asyncactor import Actor  # noqa

__all__ = [
        "ClientActor",
        "ActorState",
        "DetachedState",
        "PartialState",
        "CompleteState",
    ]

class ClientActor(Actor):
    def __init__(self, client, *a, prefix, **kw):
        super().__init__(ClientTransport(client, prefix), *a, **kw)

class ClientTransport(Transport):
    """
    This class exports the client's direct messaging interface to the
    actor.
    """
    def __init__(self, client, prefix):
        self.client = client
        self.prefix = prefix

    def monitor(self):
        return ClientMonitor(self)

    async def send(self, payload):
        await self.client.msg_send(self.prefix, payload)


class ClientMonitor(MonitorStream):
    async def __aenter__(self):
        self._mon1 = self.transport.client.msg_monitor(self.transport.prefix)
        self._mon2 = await self._mon1.__aenter__()
        return self

    def __aexit__(self, *tb):
        return self._mon1.__aexit__(*tb)

    def __aiter__(self):
        self._it = self._mon2.__aiter__()
        return self

    async def __anext__(self):
        msg = await self._it.__anext__()
        return msg.data


# The following events are used by Runner etc. to notify running jobs
# about the current connectivity state.
#
class ActorState:
    """abstract base class for states"""

    pass


@singleton
class DetachedState(ActorState):
    """I am detached, my actor group is not visible"""

    pass


@singleton
class PartialState(ActorState):
    """Some but not all members of my actor group are visible"""

    pass


@singleton
class CompleteState(ActorState):
    """All members of my actor group are visible"""

    pass
