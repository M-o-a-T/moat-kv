#
"""
Test auth method.

Does not limit anything, allows everything.
"""

import logging

log = logging.getLogger(__name__)

from . import (
    BaseServerAuthMaker,
    RootServerUser,
    BaseClientAuthMaker,
    BaseClientAuth,
    null_server_login,
    null_client_login,
)

from ..client import Client


def load(typ: str, *, make: bool = False, server: bool):
    if typ == "client":
        if server:
            return null_server_login
        else:
            return null_client_login
    if typ != "user":
        raise NotImplementedError("This module only handles users")
    if server:
        if make:
            return ServerUserMaker
        else:
            return ServerUser
    else:
        if make:
            return ClientUserMaker
        else:
            return ClientUser


class ServerUserMaker(BaseServerAuthMaker):
    name = None

    @property
    def ident(self):
        return self.name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    async def recv(cls, cmd, data):
        await cmd.send(step="GiveName")
        msg = await cmd.recv()
        assert msg.step == "HasName"
        self = cls()
        self.name = msg.name
        return self

    async def send(self, cmd):
        await cmd.send(step="SendWant")
        msg = await cmd.recv()
        assert msg.step == "WantName"
        await cmd.send(step="SendName", name=self.name, chain=self._chain.serialize(nchain=3))
        msg = await cmd.recv()

    # Annoying methods to read+save the user name from/to KV

    @classmethod
    def load(cls, data):
        self = super().load(data)
        self.name = data.name
        return self


class ServerUser(RootServerUser):
    pass


class ClientUserMaker(BaseClientAuthMaker):
    gen_schema = dict(
        type="object",
        additionalProperties=False,
        properties=dict(name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$")),
        required=["name"],
    )
    mod_schema = dict(
        type="object",
        additionalProperties=False,
        properties=dict(name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$")),
        # required=[],
    )
    name = None

    @property
    def ident(self):
        return self.name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    async def recv(cls, client: Client, ident: str, _kind: str = "user", _initial=True):
        """Read a record representing a user from the server."""
        async with client._stream(
            action="auth_get",
            typ=cls._auth_method,
            kind=_kind,
            ident=ident,
            stream=True,
            nchain=0 if _initial else 2,
        ) as s:
            m = await s.recv()
            assert m.step == "SendWant", m
            await s.send(step="WantName")
            m = await s.recv()
            assert m.step == "SendName", m
            assert m.name == ident

            self = cls(name=m.name, _initial=_initial)
            self._chain = m.chain
            return self

    async def send(self, client: Client, _kind="user"):
        """Send a record representing this user to the server."""
        async with client._stream(
            action="auth_set",
            typ=type(self)._auth_method,
            kind=_kind,
            ident=self.ident,
            stream=True,
        ) as s:
            # we could initially send the ident but don't here, for testing
            m = await s.recv()
            assert m.step == "GiveName", m
            await s.send(step="HasName", name=self.name, chain=self._chain)
            m = await s.recv()
            assert m.chain.prev is None

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        return {"name": self.name}


class ClientUser(BaseClientAuth):
    name = None

    schema = dict(
        type="object",
        additionalProperties=False,
        properties=dict(name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$")),
        required=["name"],
    )
    _name = None

    @property
    def ident(self):
        return self.name

    @classmethod
    def build(cls, user):
        self = super().build(user)
        self.name = user["name"]
        return self
