#
"""
Test auth method.

Does not limit anything, allows everything.
"""

from . import (
    BaseServerUserMaker,
    RootServerUser,
    BaseClientUserMaker,
    BaseClientUser,
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


class ServerUserMaker(BaseServerUserMaker):
    _name = None

    @property
    def ident(self):
        return self._name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    async def recv(cls, cmd, data):
        await cmd.send(step="GiveName")
        msg = await cmd.recv()
        assert msg.step == "HasName"
        self = cls()
        self._name = msg.name
        return self

    async def send(self, cmd):
        await cmd.send(step="SendWant")
        msg = await cmd.recv()
        assert msg.step == "WantName"
        await cmd.send(step="SendName", name=self._name)
        msg = await cmd.recv()

    # Annoying methods to read+save the user name from/to KV

    @classmethod
    def build(cls, data):
        self = super().build(data)
        self._name = data["UserName"]
        return self

    def save(self):
        res = super().save()
        res["UserName"] = self._name
        return res


class ServerUser(RootServerUser):
    pass


class ClientUserMaker(BaseClientUserMaker):
    schema = dict(
        type="object",
        additionalProperties=False,
        properties=dict(
            name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$")
        ),
        required=["name"],
    )
    _name = None

    @property
    def ident(self):
        return self._name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    def build(cls, user):
        self = super().build(user)
        self._name = user["name"]
        return self

    @classmethod
    async def recv(cls, client: Client, ident: str, _kind: str = "user"):
        """Read a record representing a user from the server."""
        async with client.stream(
            action="auth_get",
            typ=cls._auth_method,
            kind=_kind,
            ident=ident,
            stream=True,
        ) as s:
            m = await s.recv()
            assert m.step == "SendWant", m
            await s.send(step="WantName")
            m = await s.recv()
            assert m.name == ident

            self = cls()
            self._name = m.name
            return self

    async def send(self, client: Client, _kind="user"):
        """Send a record representing this user to the server."""
        async with client.stream(
            action="auth_set", typ=type(self)._auth_method, kind=_kind, stream=True
        ) as s:
            # we could initially send the ident but don't here, for testing
            m = await s.recv()
            assert m.step == "GiveName", m
            await s.send(step="HasName", name=self._name)
            m = await s.recv()
            assert m.changed
            assert m.prev is None

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        return {"name": self._name}


class ClientUser(BaseClientUser):
    schema = dict(
        type="object",
        additionalProperties=False,
        properties=dict(
            name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$")
        ),
        required=["name"],
    )
    _name = None

    @property
    def ident(self):
        return self._name

    @classmethod
    def build(cls, user):
        self = super().build(user)
        self._name = user["name"]
        return self
