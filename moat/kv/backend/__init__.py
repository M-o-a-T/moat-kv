from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager

import anyio

__all__ = ["get_backend", "Backend"]


class Backend(metaclass=ABCMeta):
    def __init__(self, tg):
        self._tg = tg
        self._njobs = 0
        self._ended = None

    @abstractmethod
    @asynccontextmanager
    async def connect(self, *a, **k):
        """
        This async context manager returns a connection.
        """

    async def aclose(self):
        """
        Force-close the connection.
        """
        self._tg.cancel_scope.cancel()
        if self._njobs > 0:
            with anyio.move_on_after(2):
                await self._ended.wait()

    async def spawn(self, p, *a, **kw):
        async def _run(p, a, kw, *, task_status):
            if self._ended is None:
                self._ended = anyio.Event()
            self._njobs += 1
            task_status.started()
            try:
                return await p(*a, **kw)
            finally:
                self._njobs -= 1
                if not self._njobs:
                    self._ended.set()
                    self._ended = None

        return await self._tg.start(_run, p, a, kw)

    @abstractmethod
    @asynccontextmanager
    async def monitor(self, *topic):
        """
        Return an async iterator that listens to this topic.
        """

    @abstractmethod
    async def send(self, *topic, payload):
        """
        Send this payload to this topic.
        """


def get_backend(name):
    from importlib import import_module

    if "." not in name:
        name = "moat.kv.backend." + name
    return import_module(name).connect
