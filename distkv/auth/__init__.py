# distkv.auth
# template for authorization

"""
This set of modules authenticates users.

A submodule is expected to export a "load(type:str, make:bool, server:bool)" method that returns a class.
It must recognize, at minimum:

* load("user","
"""

import jsonschema

NullSchema = { "type": "null" }

def load(typ:str, make:bool, server:bool):
    raise NotImplementedError("You need to implement me")
    
def _load_example(typ:str, make:bool, server:bool):
    """example for :proc:`load`"""
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

    The schema verifies the input of :meth:`build`.
    """
    schema = NullSchema

    @classmethod
    def build(cls, user):
        """
        Create a user record from the data conforming to this schema.
        """
        jsonschema.validate(instance=user, schema=cls.schema)
    
    async def auth(self, client: "distkv.client.Client"):
        """
        Authorizes this user with the server.
        """
        return None


class BaseClientUserMaker:
    """
    This class is used for creating a data record which builds a new user record.

    This is not the same as a ClientUser; this class is used for
    manipulating user data on the server, while a ClientUser is used solely
    for authentication.
    """
    schema = NullSchema

    @classmethod
    def build(cls, user):
        """
        Create a user record from the data conforming to this schema.
        """
        jsonschema.validate(instance=user, schema=cls.schema)

    @classmethod
    async def import(cls, client: "distkv.client.Client", data):
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

    async def login(self, data):
        """Verify that this record authenticates this user"""
        jsonschema.validate(instance=user, schema=cls.schema)

    @classmethod
    async def read(self, data):
        """Create a ServerUser object from existing stored data"""
        pass

    async def export(self):
        """Returns a record representing the user that can be sent to the client"""
        return None

    
class BaseServerUserMaker:
    """
    This class is used on the server to verify the user record and to store it in DistKV.

    The schema verifies the output of :meth:`BaseClientUserMaker.export`.
    It does *not* verify the user's data record in DistKV.
    """
    schema = NullSchema

    @classmethod
    async def build(cls, data) -> BaseServerUserMaker:
        """Create a new user by reading the record from the client"""
        jsonschema.validate(instance=user, schema=cls.schema)

    @classmethod
    async def read(cls, data):
        """Read the user data from DistKV"""
        pass

    async def save(self):
        """Create a record to represent this user, suitable for saving to DistKV"""
        pass

