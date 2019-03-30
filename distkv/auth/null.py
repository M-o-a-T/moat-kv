#
"""
Null auth method.

Does not limit anything, allows everything.
"""

from . import BaseServerUserMaker,RootServerUser,BaseClientUserMaker,BaseClientUser, null_server_login,null_client_login

def load(typ:str, *, make:bool=False, server:bool):
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
    pass
class ServerUser(RootServerUser):
    pass
class ClientUserMaker(BaseClientUserMaker):
    pass
class ClientUser(BaseClientUser):
    pass
