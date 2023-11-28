#
"""
Password-based auth method.

Does not limit anything, allows everything.
"""

import nacl.secret

from ..client import Client, NoData
from ..exceptions import AuthFailedError
from ..model import Entry
from ..server import StreamCommand
from . import (
    BaseClientAuth,
    BaseClientAuthMaker,
    BaseServerAuthMaker,
    RootServerUser,
    null_client_login,
    null_server_login,
)


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


async def pack_pwd(client, password, length):
    """Client side: encrypt password"""
    secret = await client.dh_secret(length=length)
    from hashlib import sha256

    pwd = sha256(password).digest()
    box = nacl.secret.SecretBox(secret)
    pwd = box.encrypt(pwd)
    return pwd


async def unpack_pwd(client, password):
    """Server side: extract password"""
    box = nacl.secret.SecretBox(client.dh_key)
    pwd = box.decrypt(password)
    return pwd
    # TODO check with Argon2


class ServerUserMaker(BaseServerAuthMaker):
    _name = None
    _aux = None
    password: str = None

    @property
    def ident(self):
        return self._name

    @classmethod
    async def recv(cls, cmd, data):
        self = cls()
        self._name = data["ident"]
        self._aux = data.get("aux")
        pwd = data.get("password")
        pwd = await unpack_pwd(cmd.client, pwd)

        # TODO use Argon2 to re-hash this
        self.password = pwd
        return self

    async def send(self, cmd):
        return  # nothing to do, we don't share the hash

    @classmethod
    def load(cls, data):
        self = super().load(data)
        self._name = data.path[-1]
        return self

    def save(self):
        res = super().save()
        res["password"] = self.password
        return res


class ServerUser(RootServerUser):
    @classmethod
    def load(cls, data: Entry):
        """Create a ServerUser object from existing stored data"""
        self = super().load(data)
        self._name = data.name
        return self

    async def auth(self, cmd: StreamCommand, data):
        """Verify that @data authenticates this user."""
        await super().auth(cmd, data)

        pwd = await unpack_pwd(cmd.client, data.password)
        if pwd != self.password:  # pylint: disable=no-member
            # pylint: disable=no-member
            raise AuthFailedError("Password hashes do not match", self._name)


class ClientUserMaker(BaseClientAuthMaker):
    gen_schema = dict(
        type="object",
        additionalProperties=True,
        properties=dict(
            name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$"),
            password=dict(type="string", minLength=5),
        ),
        required=["name", "password"],
    )
    mod_schema = dict(
        type="object",
        additionalProperties=True,
        properties=dict(password=dict(type="string", minLength=5)),
        # required=[],
    )
    _name = None
    _pass = None
    _length = 1024

    @property
    def ident(self):
        return self._name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    def build(cls, user, _initial=True):
        self = super().build(user, _initial=_initial)
        self._name = user["name"]
        if "password" in user:
            self._pass = user["password"].encode("utf-8")
        return self

    @classmethod
    async def recv(cls, client: Client, ident: str, _kind: str = "user", _initial=True):
        """Read a record representing a user from the server."""
        m = await client._request(
            action="auth_get",
            typ=cls._auth_method,
            kind=_kind,
            ident=ident,
            nchain=0 if _initial else 2,
        )
        # just to verify that the user exists
        # There's no reason to send the password hash back
        self = cls(_initial=_initial)
        self._name = m.name
        try:
            self._chain = m.chain
        except AttributeError:
            pass
        return self

    async def send(self, client: Client, _kind="user", **msg):  # pylint: disable=unused-argument,arguments-differ
        """Send a record representing this user to the server."""
        if self._pass is not None:
            msg["password"] = await pack_pwd(client, self._pass, self._length)

        await client._request(
            action="auth_set",
            ident=self._name,
            typ=type(self)._auth_method,
            kind=_kind,
            chain=self._chain,
            **msg,
        )

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        res = super().export()
        res["name"] = self._name
        return res


class ClientUser(BaseClientAuth):
    schema = dict(
        type="object",
        additionalProperties=True,
        properties=dict(
            name=dict(type="string", minLength=1, pattern="^[a-zA-Z][a-zA-Z0-9_]*$"),
            password=dict(type="string", minLength=5),
        ),
        required=["name", "password"],
    )
    _name = None
    _pass = None
    _length = 1024

    @property
    def ident(self):
        return self._name

    @classmethod
    def build(cls, user):
        self = super().build(user)
        self._name = user["name"]
        self._pass = user["password"].encode("utf-8")
        return self

    async def auth(self, client: Client, chroot=()):
        """
        Authorizes this user with the server.
        """
        try:
            pw = await pack_pwd(client, self._pass, self._length)
            await client._request(
                action="auth",
                typ=self._auth_method,
                iter=False,
                ident=self.ident,
                password=pw,
                **self.auth_data(),
            )
        except NoData:
            pass
