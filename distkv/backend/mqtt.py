import anyio
from distmqtt.client import MQTTClient
from distmqtt.codecs import NoopCodec
from contextlib import asynccontextmanager
from . import Backend

# Simply setting connect=asyncserf.serf_client interferes with mocking
# when testing.

class MqttMessage:
    def __init__(self,topic,payload):
        self.topic = topic
        self.payload = payload

class MqttBackend(Backend):
    @asynccontextmanager
    async def connect(self, **kw):
        C = MQTTClient(self._tg, codec=NoopCodec())
        try:
            await C.connect(**kw)
            self.client = C
            yield self
        finally:
            self.client = None
            async with anyio.open_cancel_scope(shield=True):
                await C.disconnect()

    @asynccontextmanager
    async def monitor(self, *topic):
        topic = '/'.join(topic)
        async with self.client.subscription(topic) as sub:
            async def sub_get(sub):
                async for msg in sub:
                    yield MqttMessage(msg.topic.split('/'), msg.data)
            yield sub_get(sub)

    def send(self, *topic, payload):
        """
        Send this payload to this topic.
        """
        return self.client.publish('/'.join(topic), message=payload)

@asynccontextmanager
async def connect(**kw):
    async with anyio.create_task_group() as tg:
        c = MqttBackend(tg)
        async with c.connect(**kw):
            yield c

