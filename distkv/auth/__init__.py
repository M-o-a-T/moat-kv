# distkv.auth
# template for authorization

"""
This set of modules authenticates users.

A submodule is expected to export a "load(type:str, make:bool, server:bool)" method that returns a class.
It must recognize, at minimum:

* load("stream", server:bool):

  A filter used for logging in a user.

* load("user", server:bool, make:bool)

  A class used to represent the user, or a way to create/manipulate a user record.

The client process is:

* create a user:

  * Create a :cls:`BaseUserMaker` by calling :meth:`BaseUserMaker.build`
    with a record conforming to its schema.

  * Export that and save it to the server, at (None,"auth","user",NAME).

* modify a user:

  * Call :cls:`BaseUserMaker.import` with the value from the server.

* Log in:

  * Create a :cls:`BaseUser` by calling :meth:`BaseUserMaker.build`
    with a record conforming to its schema.

  * Call :meth:`BaseUser.auth`.

The server process is:

* create a user:

  * The server intercepts the write call and uses the data to call :meth:`BaseServerUserMaker.build`.

  * It calls :meth:`BaseServerMaker.save` and stores the actual result.

* modify a user:

  * the server calls :meth:`BaseServerUser.read` with the stored data,
    then sends a record created with :meth:`BaseServerUser.save` to the client.

* verify a user:

  * The server calls :meth:`BaseServerUser.read` with the stored data.
     
  * The server calls :meth:`BaseServerUser.auth` with the record from the client.

"""

import jsonschema
from importlib import import_module
from ..client import NoData
from ..util import split_one
from ..exceptions import NoAuthModuleError

NullSchema = { "type": "object", "additionalProperties":False }

def loader(method:str, *a,**k):
    m = method
    if '.' not in m:
        m= 'distkv.auth.'+m
    cls = import_module(m).load(*a,**k)
    cls._auth_method = method
    return cls


def gen_auth(s: str):
    """
    Generate auth data from parameters or YAML file (if first char is '=').
    """
    from distkv.auth import loader
    m,*p = s.split()
    if len(p) == 0 and m[0] == '=':
        with io.open(m[1:],"r") as f:
            kw = yaml.safe_load(f)
            m = kw.pop('type')
    else:  
        kw = {}
        for pp in p:
            split_one(pp,kw)
    try:
        m = loader(m, "user", server=False)
    except ModuleNotFoundError:
        raise NoAuthModuleError(m) from None
    return m.build(kw)


def load(typ:str, *, make:bool=False, server:bool):
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
    raise NotImplementedError("You need to implement me")
    
async def null_server_login(stream):
    return stream

async def null_client_login(stream, user:'BaseClientUser'):
    return stream

def _load_example(typ:str, make:bool, server:bool):
    """example for :proc:`load`"""
    if typ == "client":
        if server:
            return null_server_login
        else:
            return null_client_login
    if typ != "user":
        raise NotImplementedError("This module only handles users")
    if server:
        if make:
            return BaseServerUserMaker
        else:
            return BaseServerUser
    else:
        if make:
            return BaseClientUserMaker
        else:
            return BaseClientUser


class BaseClientUser:
    """
    This class is used for creating a data record which authenticates a user.

    The schema verifies the input to :meth:`build`.
    """
    schema = NullSchema

    @classmethod
    def build(cls, user):
        """
        Create a user record from the data conforming to this schema.
        """
        jsonschema.validate(instance=user, schema=cls.schema)
        return cls()
    
    @property
    def ident(self):
        """Some user identifier.
        Required so that the server can actually find the record.
        """
        return "*"

    async def auth(self, client: "distkv.client.Client", chroot=()):
        """
        Authorizes this user with the server.
        """
        try:
            await client.request(action="auth", typ=self._auth_method, iter=False, ident=self.ident, **self.auth_data())
        except NoData:
            pass

    def auth_data(self):
        """
        Additional data for the initial auth message.

        Does NOT include 'ident', that gets added explicitly by :meth:`auth`.
        """
        return {}


class BaseClientUserMaker:
    """
    This class is used for creating a data record which describes a user record.

    This is not the same as a :cls:`BaseClientUser`; this class is used to
    represent stored user data on the server, while a :cls:`BaseClientUser` is used solely
    for authentication.

    The schema verifies the input to :meth:`build`.
    """
    schema = NullSchema

    @classmethod
    def build(cls, user):
        """
        Create a user record from the data conforming to this schema.
        """
        jsonschema.validate(instance=user, schema=cls.schema)
        return cls()

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        return {}

    @property
    def ident(self):
        """Some user identifier.
        Required so that the server can actually find the record.
        """
        return "*"

    @classmethod
    async def recv(cls, client: "distkv.client.Client",  ident:str, _kind='user'):
        res = await client.request("auth_get", typ=type(self)._auth_method, kind=_kind)
        """Read this user from the server."""
        return cls()
    
    async def send(self, client: "distkv.client.Client", _kind='user'):
        """Send this user to the server."""
        try:
            await client.request("auth_set", iter=False, typ=type(self)._auth_method, kind=_kind, **self.send_data())
        except NoData:
            pass

    def send_data(self):
        return {}

class BaseServerUser:
    """
    This class is used on the server to represent / verify a user.

    The schema verifies the output of :class:`BaseClientUser`.
    It does *not* verify the user's data record in DistKV.

    The schema matches the initial client message and thus needs to
    have additionalProperties set.
    """
    schema = NullSchema.copy()
    schema['additionalProperties'] = True

    is_super_root = False
    can_create_subtree = False
    can_auth_read = False
    can_auth_write = False

    @classmethod
    async def build(cls, data: 'distkv.model.Entry'):
        """Create a ServerUser object from existing stored data"""
        return cls()

    async def auth(self, cmd: 'distkv.server.StreamCommand', data):
        """Verify that @data authenticates this user."""
        jsonschema.validate(instance=data, schema=type(self).schema)

    def info(self):
        """
        Return whatever public data the user might want to have displayed.

        This includes information to identify the user, but not anything
        that'd be suitable for verifying or even faking authorization.
        """
        return {}

    async def check_read(self, *path, client: "distkv.client.ServerClient", data=None):
        """Check that this user may read the element at this location.
        This method may modify the data.
        """
        return data

    async def check_write(self, *path, client: "distkv.client.ServerClient", data=None):
        """Check that this user may write the element at this location.
        This method may modify the data.
        """
        return data

class RootServerUser(BaseServerUser):
    """The default user when no auth is required
    """
    is_super_root = True
    can_create_subtree = True
    can_auth_read = True
    can_auth_write = True

class BaseServerUserMaker:
    """
    This class is used on the server to verify the user record and to store it in DistKV.

    The schema verifies the output of :meth:`BaseClientUserMaker.save`.
    It does *not* verify the user's data record in DistKV.
    """
    schema = NullSchema.copy()
    schema['additionalProperties'] = True

    @classmethod
    def build(cls, data: 'distkv.model.Entry'):
        """Read the user data from DistKV"""
        return cls()

    @classmethod
    async def recv(cls, cmd: 'distkv.server.StreamCommand', data) -> 'BaseServerUserMaker':
        """Create a new user by reading the record from the client"""
        jsonschema.validate(instance=data, schema=cls.schema)
        return cls()

    @property
    def ident(self):
        """The record to store this user under."""
        return '*'

    def save(self):
        """Return a record to represent this user, suitable for saving to DistKV"""
        return {}

    async def send(self, cmd: 'distkv.server.StreamCommand'):
        """Send a record to the client, possibly multi-step secured"""
        return {}

