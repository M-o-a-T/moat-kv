"""
An online-updated config store

"""

try:
    from contextlib import asynccontextmanager
except ImportError:  # pragma: no cover
    from async_generator import asynccontextmanager

import logging

from .exceptions import ServerClosedError, ServerError
from .obj import ClientEntry, ClientRoot

logger = logging.getLogger(__name__)


class ConfigEntry(ClientEntry):
    @classmethod
    def child_type(cls, name):  # pragma: no cover
        logger.warning("Online config sub-entries are ignored")
        return ClientEntry

    async def set_value(self, value):
        await self.root.client.config._update(self._name, value)


class ConfigRoot(ClientRoot):
    CFG = "config"

    @classmethod
    def child_type(cls, name):
        return ConfigEntry

    @asynccontextmanager
    async def run(self):
        try:
            async with super().run() as x:
                yield x
        except ServerClosedError:  # pragma: no cover
            pass
        except ServerError:  # pragma: no cover
            logger.exception("No config data")
