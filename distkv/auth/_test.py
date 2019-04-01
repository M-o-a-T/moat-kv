#
"""
Test auth method.

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
    _name=None
    @property
    def ident(self):
        return self._name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    async def recv(self,cmd,data):
        cmd.send(step="GiveName")
        msg = await cmd.recv()
        assert msg.step == "HasName"
        self._name = msg.name

    async def send(self,cmd):
        cmd.send(step="SendWant")
        msg = await cmd.recv()
        assert msg.step == "WantName"
        cmd.send(step="SendName",name=self._name)

    # Annoying methods to read+save the user name

    @classmethod
    def build(cls,data):
        self=super().build(data)
        self._name = data['UserName']

    def save(self):
        res = super().save()
        res['UserName']=self._name
        return res

class ServerUser(RootServerUser):
    pass

class ClientUserMaker(BaseClientUserMaker):
    _name=None
    @property
    def ident(self):
        return self._name

    # Overly-complicated methods of exchanging the user name

    @classmethod
    def build(cls,user):
        self=super().build(user)
        self._name=user['name']

    @classmethod
    async def recv(cls, client: "distkv.client.Client", ident:str, _kind:'user'):
        """Read a record representing a user from the server."""
        s = await client.stream(action="auth_get",typ=cls._auth_method,kind=_kind,ident=ident)
        m = await s.recv()
        assert m.step == "SendWant", m
        await s.send(step="WantName")
        m = await s.recv()
        assert m.name == ident

        self.= cls()
        self._name = m.name
        return self
    
    async def save(self, client: "distkv.client.Client", _kind='user'):
        """Send a record representing this user to the server."""
        s = await client.stream(action="auth_set",typ=cls._auth_method,kind=_kind)
        # we could initially send the ident but don't here, for testing
        m = await s.recv()
        assert m.step == "GiveName", m
        await m.send(step="HasName", name=self._name)

    def export(self):
        """Return the data required to re-create the user via :meth:`build`."""
        return {'name':self._name}

class ClientUser(BaseClientUser):
    pass
