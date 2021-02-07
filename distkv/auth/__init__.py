# distkv.auth
# template for authorization

"""
This set of modules authenticates users.

A submodule is expected to export a "load(type:str, make:bool, server:bool)"
method that returns a class.

It must recognize, at minimum:

* load("stream", server:bool):

  A filter used for logging in a user.

* load("user", server:bool, make:bool)

  A class used to represent the user, or a way to create/manipulate a user record.

The client process is:

* create a user:

  * Create a :class:`BaseUserMaker` by calling :meth:`BaseUserMaker.build`
    with a record conforming to its schema.

  * Export that and save it to the server, at (None,"auth","user",NAME).

* modify a user:

  * Call :class:`BaseUserMaker.import` with the value from the server.

* Log in:

  * Create a :class:`BaseUser` by calling :meth:`BaseUserMaker.build`
    with a record conforming to its schema.

  * Call :meth:`BaseUser.auth`.

The server process is:

* create a user:

  * The server intercepts the write call and uses the data to call
    :meth:`BaseServerAuthMaker.build`.

  * It calls :meth:`BaseServerMaker.save` and stores the actual result.

* modify a user:

  * the server calls :meth:`BaseServerAuth.read` with the stored data,
    then sends a record created with :meth:`BaseServerAuth.save` to the client.

* verify a user:

  * The server calls :meth:`BaseServerAuth.read` with the stored data.

  * The server calls :meth:`BaseServerAuth.auth` with the record from the client.

"""

import jsonschema
import io
from importlib import import_module
from ..client import NoData, Client
from ..model import Entry
from ..server import StreamCommand, ServerClient
from ..util import split_arg, attrdict, NotGiven, yload, Path
from ..exceptions import NoAuthModuleError
from ..types import ACLFinder, NullACL

# Empty schema
null_schema = {"type": "object", "additionalProperties": False}

# Additional schema data for specific types
add_schema = {
    "user": {
        "acl": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"key": {type: "string", "minLength": 1}},
        },
        "conv": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"key": {type: "string", "minLength": 1}},
        },
    }
}


def loader(method: str, typ: str, *a, **k):
    m = method
    if "." not in m:
        m = "distkv.auth." + m
    cls = import_module(m).load(typ, *a, **k)
    cls._auth_method = method
    cls._auth_typ = typ
    cls.aux_schemas = add_schema.get(typ, null_schema)
    return cls


def gen_auth(s: str):
    """
    Generate auth data from parameters or YAML file (if first char is '=').
    """
    if not isinstance(s, str):
        return s  # called twice. Oh well.

    m, *p = s.split()
    if len(p) == 0 and m[0] == "=":
        with io.open(m[1:], "r") as f:
            kw = yload(f)
            m = kw.pop("type")
    else:
        kw = {}
        for pp in p:
            split_arg(pp, kw)
    try:
        m = loader(m, "user", server=False)
    except ModuleNotFoundError:
        raise NoAuthModuleError(m) from None
    return m.build(kw)


def load(typ: str, *, make: bool = False, server: bool):
    """
    This procedure is used to load and return a user management class.

    Arguments:
        typ: the type of module to load.
        make: flag that the caller wants a record-generating, not a
              record-using class.
        server: flag that the class is to be used on the server, not on the
                client.

    Types:
        stream: the filter used for authorizing the user.
        user: represents a user record.
    """
    raise NotImplementedError("You need to implement me")  # pragma: no cover


async def null_server_login(stream):
    return stream


async def null_client_login(stream, user: "BaseClientAuth"):  # pylint: disable=unused-argument
    return stream


def _load_example(typ: str, make: bool, server: bool):  # pragma: no cover
    """example code for :proc:`load`"""
    if typ == "client":
        if server:
            return null_server_login
        else:
            return null_client_login
    if typ != "user":
        raise NotImplementedError("This module only handles users")
    if server:
        if make:
            return BaseServerAuthMaker
        else:
            return BaseServerAuth
    else:
        if make:
            return BaseClientAuthMaker
        else:
            return BaseClientAuth


class _AuthLoaded:
    # This class is mainly there to appease pylint
    _auth_method = None


class BaseClientAuth(_AuthLoaded):
    """
    This class is used for creating a data record which authenticates a user.

    The schema verifies the input to :meth:`build`.
    """

    schema = null_schema

    def __init__(self, **data):
        jsonschema.validate(instance=data, schema=type(self).schema)
        for k, v in data.items():
            setattr(self, k, v)

    @classmethod
    def build(cls, user):
        """
        Create a user record from the data conforming to this schema.
        """
        return cls(**user)

    @property
    def ident(self):
        """Some user identifier.
        Required so that the server can actually find the record.
        """
        return "*"

    async def auth(self, client: Client, chroot=()):
        """
        Authorizes this record with the server.
        """
        try:
            await client._request(
                action="auth",
                typ=self._auth_method,
                iter=False,
                chroot=chroot,
                ident=self.ident,
                data=self.auth_data(),
            )
        except NoData:
            pass

    def auth_data(self):
        """
        Additional data for the initial auth message.

        Does NOT include 'ident', that gets added explicitly by :meth:`auth`.
        """
        return {}


class BaseClientAuthMaker(_AuthLoaded):
    """
    This class is used for creating a data record which describes a user record.

    While :class:`BaseClientAuth` is used solely for authentication,
    this class is used to represent the server's user data.

    The schema verifies the input to :meth:`build`.
    """

    gen_schema = null_schema
    mod_schema = null_schema
    _chain = None

    def __init__(self, _initial=True, **data):
        if _initial:
            jsonschema.validate(instance=data, schema=type(self).gen_schema)
        else:
            jsonschema.validate(instance=data, schema=type(self).mod_schema)
        for k, v in data.items():
            setattr(self, k, v)

    @classmethod
    def build(cls, user, _initial=True):
        """
        Create a user record from the data conforming to this schema.
        """
        return cls(**user, _initial=_initial)

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        return {}  # pragma: no cover

    @property
    def ident(self):
        """The identifier for this user.

        Required so that the server can actually find the record.
        """
        return "*"  # pragma: no cover

    @classmethod
    async def recv(cls, client: Client, ident: str, _kind="user", _initial=True):
        """Read this user from the server.

        Sample code …
        """
        # pragma: no cover
        res = await client._request(
            "auth_get", typ=cls._auth_method, kind=_kind, ident=ident, nchain=0 if _initial else 2
        )
        self = cls(_initial=_initial)
        self._chain = res.chain
        return self

    async def send(self, client: Client, _kind="user"):
        """Send this user to the server."""
        try:
            await client._request(
                "auth_set",
                iter=False,
                typ=type(self)._auth_method,
                kind=_kind,
                ident=self.ident,
                chain=self._chain,
                data=self.send_data(),
            )
        except NoData:
            pass

    def send_data(self):
        return {}


class BaseServerAuth(_AuthLoaded):
    """
    This class is used on the server to represent / verify a user.

    The schema verifies whatever data the associated ``ClientAuth`` initially sends.
    """

    schema = null_schema.copy()
    schema["additionalProperties"] = True

    is_super_root = False
    can_create_subtree = False
    can_auth_read = False
    can_auth_write = False

    def __init__(self, data: dict = {}):  # pylint: disable=dangerous-default-value
        if data:
            for k, v in data.items():
                setattr(self, k, v)

    @classmethod
    def load(cls, data: Entry):
        """Create a ServerAuth object from existing stored data"""
        return cls(data.data)

    async def auth(self, cmd: StreamCommand, data):  # pylint: disable=unused-argument
        """Verify that @data authenticates this user."""
        jsonschema.validate(instance=data.get("data", {}), schema=type(self).schema)

    def aux_conv(self, data: Entry, root: Entry):
        from ..types import ConvNull

        try:
            data = data["conv"].data["key"]
            res, _ = root.follow_acl(Path(None, "conv", data), create=False, nulls_ok=True)
            return res
        except (KeyError, AttributeError):
            return ConvNull

    def aux_acl(self, data: Entry, root: Entry):
        try:
            data = data["acl"].data["key"]
            if data == "*":
                return NullACL
            acl, _ = root.follow_acl(Path(None, "acl", data), create=False, nulls_ok=True)
            return ACLFinder(acl)
        except (KeyError, AttributeError):
            return NullACL

    def info(self):
        """
        Return whatever public data the user might want to have displayed.

        This includes information to identify the user, but not anything
        that'd be suitable for verifying or even faking authorization.
        """
        return {}

    async def check_read(
        self, *path, client: ServerClient, data=None
    ):  # pylint: disable=unused-argument
        """Check that this user may read the element at this location.
        This method may modify the data.
        """
        return data

    async def check_write(
        self, *path, client: ServerClient, data=None
    ):  # pylint: disable=unused-argument
        """Check that this user may write the element at this location.
        This method may modify the data.
        """
        return data


class RootServerUser(BaseServerAuth):
    """The default user when no auth is required

    Interim record. TODO: create a separate ACL thing.
    """

    is_super_root = True
    can_create_subtree = True
    can_auth_read = True
    can_auth_write = True


class BaseServerAuthMaker(_AuthLoaded):
    """
    This class is used on the server to verify the transmitted user record
    and to store it in DistKV.

    The schema verifies the data from the client.
    """

    schema = null_schema
    aux_schemas = None  # set by the loader

    def __init__(self, chain=None, data=None):
        if data is not None and data is not NotGiven:
            for k, v in data.items():
                setattr(self, k, v)
        self._chain = chain

    @classmethod
    def load(cls, data: Entry):
        """Read the user data from DistKV"""
        return cls(chain=data.chain, data=data.data)

    @classmethod
    async def recv(
        cls, cmd: StreamCommand, data: attrdict  # pylint: disable=unused-argument
    ) -> "BaseServerAuthMaker":
        """Create/update a new user by reading the record from the client"""
        dt = data.get("data", None) or {}
        jsonschema.validate(instance=dt, schema=cls.schema)
        self = cls(chain=data["chain"], data=dt)
        return self

    @property
    def ident(self):
        """The record to store this user under."""
        return "*"

    def save(self):
        """Return a record to represent this user, suitable for saving to DistKV"""
        # does NOT contain "ident" or "chain"!
        return {}

    async def send(self, cmd: StreamCommand):  # pylint: disable=unused-argument
        """Send a record to the client, possibly multi-step / secured / whatever"""
        res = {}
        res["chain"] = self._chain.serialize() if self._chain else None
        return res
