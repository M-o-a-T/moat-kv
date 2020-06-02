from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager

__all__ = ["get_backend", "Backend"]


class Backend(metaclass=ABCMeta):
    def __init__(self, tg):
        self._tg = tg

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
        await self._tg.cancel_scope.cancel()

    def spawn(self, *a, **kw):
        return self._tg.spawn(*a, **kw)

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
        name = "distkv.backend." + name
    return import_module(name).connect
