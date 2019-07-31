"""
An online-updated config store

"""

import anyio

from .entry import ClientRoot, ClientEntry

import logging
logger = logging.getLogger(__name__)


class ConfigEntry(ClientEntry):
    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        logger.warning("Online config sub-entries are ignored")
        return ClientEntry

    async def set_value(self, v):
        self.root.client.config._update(self._name, v)


class ConfigRoot(ClientRoot):
    CFG = "config"

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        return ConfigEntry

