import anyio
from distmqtt.client import MQTTClient
from distmqtt.codecs import NoopCodec
from contextlib import asynccontextmanager
from . import Backend
import logging

logger = logging.getLogger(__name__)

# Simply setting connect=asyncserf.serf_client interferes with mocking
# when testing.


class MqttMessage:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


class MqttBackend(Backend):
    client = None

    @asynccontextmanager
    async def connect(self, *a, **kw):
        C = MQTTClient(self._tg, codec=NoopCodec())
        try:
            await C.connect(*a, **kw)
            self.client = C
            yield self
        finally:
            self.client = None
            async with anyio.open_cancel_scope(shield=True):
                await C.disconnect()

    @asynccontextmanager
    async def monitor(self, *topic):
        topic = "/".join(topic)
        logger.error("Monitor %s start", topic)
        try:
            async with self.client.subscription(topic) as sub:

                async def sub_get(sub):
                    async for msg in sub:
                        yield MqttMessage(msg.topic.split("/"), msg.data)

                yield sub_get(sub)
        except BaseException as exc:
            exx = exc
        else:
            exx = None
        finally:
            logger.error("Monitor %s end: %r", topic, exx)

    def send(self, *topic, payload):  # pylint: disable=invalid-overridden-method
        """
        Send this payload to this topic.
        """
        # client.publish is also async, pass-thru
        return self.client.publish("/".join(topic), message=payload)


@asynccontextmanager
async def connect(**kw):
    async with anyio.create_task_group() as tg:
        c = MqttBackend(tg)
        async with c.connect(**kw):
            yield c
