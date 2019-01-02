from async_generator import asynccontextmanager
import anyio

_serfs = set()

@asynccontextmanager
async def mock_serf_client(**cfg):
    async with anyio.create_task_group() as tg:
        ms = MockServ(tg, **cfg)
        _serfs.add(ms)
        try:
            yield ms
        finally:
            _serfs.remove(ms)

class MockServ:
    def __init__(self, tg, **cfg):
        self.cfg = cfg
        self.tg = tg
        self.streams = {}

    def __hash__(self):
        return id(self)

    def spawn(self, *args, **kw):
        return self.tg.spawn(*args, **kw)

    def stream(self, event_types='*'):
        if ',' in event_types or not event_types.startswith('user:'):
            raise RuntimeError("not supported")
        s = MockSerfStream(self, event_types)
        return s

    async def event(self, typ, payload, coalesce=False):
        for s in list(_serfs):
            s = s.streams.get(typ, None)
            if s is not None:
                await s.q.put(payload)

class MockSerfStream:
    def __init__(self, serf, typ):
        self.serf = serf
        self.typ = typ

    async def __aenter__(self):
        if typ in self.serf.streams:
            raise RuntimeError("Only one listener per event type allowed")
        self.q = anyio.create_queue(100)
        self.streams[self.typ] = self

    async def __aexit__(self, *tb):
        del self.streams[self.typ]
        del self.q

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.q.get()

