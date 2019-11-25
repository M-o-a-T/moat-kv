import anyio
import asyncserf
from contextlib import asynccontextmanager
from . import Backend

# Simply setting connect=asyncserf.serf_client interferes with mocking
# when testing.

class SerfBackend(Backend):
    @asynccontextmanager
    async def connect(self, *a, **k):
        async with asyncserf.serf_client(*a,**k) as c:
            self.client = c
            try:
                yield self
            finally:
                self.client = None

    def monitor(self, *topic):
        topic = "user:" + '.'.join(topic)
        return self.client.stream(topic)

    def send(self, *topic, payload):
        """
        Send this payload to this topic.
        """
        return self.client.event('.'.join(topic), payload=payload, coalesce=False)

@asynccontextmanager
async def connect(*a,**kw):
    async with anyio.create_task_group() as tg:
        c = SerfBackend(tg)
        async with c.connect(*a, **kw):
            yield c

