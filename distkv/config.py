"""
An online-updated config store

"""

try:
    from contextlib import asynccontextmanager
except ImportError:  # pragma: no cover
    from async_generator import asynccontextmanager

from .errors import ServerError
from .obj import ClientRoot, ClientEntry

import logging

logger = logging.getLogger(__name__)


class ConfigEntry(ClientEntry):
    @classmethod
    def child_type(cls, name):  # pragma: no cover
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        logger.warning("Online config sub-entries are ignored")
        return ClientEntry

    async def set_value(self, value):
        await self.root.client.config._update(self._name, value)


class ConfigRoot(ClientRoot):
    CFG = "config"

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        return ConfigEntry

    @asynccontextmanager
    async def run(self):
        try:
            async with super().run() as x:
                yield x
        except ServerError:  # pragma: no cover
            logger.exception("No config data")
