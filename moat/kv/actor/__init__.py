"""
This module implements a :class:`asyncactor.Actor` which works on top of
a MoaT-KV client.
"""

from asyncactor import Actor  # noqa
from asyncactor.abc import MonitorStream, Transport

__all__ = [
    "ClientActor",
    "ActorState",
    "BrokenState",
    "DetachedState",
    "PartialState",
    "CompleteState",
]


class ClientActor(Actor):
    def __init__(self, client, *a, topic, **kw):
        super().__init__(ClientTransport(client, topic), *a, **kw)


class ClientTransport(Transport):
    """
    This class exports the client's direct messaging interface to the
    actor.
    """

    def __init__(self, client, topic):
        self.client = client
        self.topic = topic

    def monitor(self):
        return ClientMonitor(self)

    async def send(self, payload):
        await self.client.msg_send(self.topic, payload)


class ClientMonitor(MonitorStream):
    _mon1 = None
    _mon2 = None
    _it = None

    async def __aenter__(self):
        self._mon1 = self.transport.client.msg_monitor(self.transport.topic)
        self._mon2 = await self._mon1.__aenter__()
        return self

    async def __aexit__(self, *tb):
        return await self._mon1.__aexit__(*tb)

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
    """base class for states"""

    def __init__(self, msg=None):
        self.msg = msg

    def __repr__(self):
        return "<%s:%r>" % (self.__class__.__name__, self.msg)


class BrokenState(ActorState):
    """I have no idea what's happening, probably nothing good"""

    pass


class DetachedState(ActorState):
    """I am detached, my actor group is not visible"""

    pass


class PartialState(ActorState):
    """Some but not all members of my actor group are visible"""

    pass


class CompleteState(ActorState):
    """All members of my actor group are visible"""

    pass
