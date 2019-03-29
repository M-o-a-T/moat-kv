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
    then sends a record created with :meth:`BaseServerUser.export` to the client.

* verify a user:

  * The server calls :meth:`BaseServerUser.read` with the stored data.
     
  * The server calls :meth:`BaseServerUser.auth` with the record from the client.

"""

import jsonschema
from importlib import import_module


NullSchema = { "type": "null" }

def loader(method:str, *a,**k):
    m = method
    if m[0] == '_':
        m= 'distkv.auth.'+m[1:]
    obj = import_module(m).load(*a,**k)
    obj._auth_method = method

def load(typ:str, make:bool, server:bool):
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
        return "anon"

    async def __call__(self, client: "distkv.client.Client", chroot=()):
        """
        Authorizes this user with the server.
        """
        return None


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

    @classmethod
    async def load(cls, client: "distkv.client.Client", data):
        """Read a record representing a user from the server."""
        pass
    
    async def export(self, client: "distkv.client.Client"):
        """Create a record representing this user, to send to the server."""
        return None


class BaseServerUser:
    """
    This class is used on the server to represent / verify a user.

    The schema verifies the output of :class:`BaseClientUser`.
    It does *not* verify the user's data record in DistKV.
    """
    schema = NullSchema

    @classmethod
    async def read(self, auth: 'distkv.model.Entry', name:str):
        """Create a ServerUser object from existing stored data"""
        return cls()

    async def auth(self, cmd: 'distkv.server.StreamCommand', data):
        """Verify that this record authenticates this user"""
        jsonschema.validate(instance=data, schema=cls.schema)

    async def auth_path(*path, client: "distkv.client.ServerClient", mode="r"):
        """Verify that this user may access/write this path"""
        return True

    async def verify_write(*path, client: "distkv.client.ServerClient", data=None):
        """Check that this user may write these data at this location.
        This method may modify the data.
        """
        return data

class BaseServerUserMaker:
    """
    This class is used on the server to verify the user record and to store it in DistKV.

    The schema verifies the output of :meth:`BaseClientUserMaker.export`.
    It does *not* verify the user's data record in DistKV.
    """
    schema = NullSchema

    @classmethod
    async def build(cls, data) -> 'BaseServerUserMaker':
        """Create a new user by reading the record from the client"""
        jsonschema.validate(instance=user, schema=cls.schema)
        return cls()

    async def save(self):
        """Return a record to represent this user, suitable for saving to DistKV"""
        pass

    @classmethod
    async def read(cls, data):
        """Read the user data from DistKV"""
        return cls()

    async def export(self):
        """Return a record to represent this user, suitable for sending to the client"""
        pass
