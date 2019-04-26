#
"""
Null auth method.

Does not limit anything, allows everything.
"""

from . import (
    BaseServerAuthMaker,
    BaseClientAuthMaker,
    BaseClientAuth,
    RootServerUser,
    null_server_login,
    null_client_login,
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


class ServerUserMaker(BaseServerAuthMaker):
    schema = {"type": "object", "additionalProperties": False}


class ServerUser(RootServerUser):
    schema = {"type": "object", "additionalProperties": False}


class ClientUserMaker(BaseClientAuthMaker):
    schema = {"type": "object", "additionalProperties": False}

    @property
    def ident(self):
        return "*"


class ClientUser(BaseClientAuth):
    schema = {"type": "object", "additionalProperties": False}
