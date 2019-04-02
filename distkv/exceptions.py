class DistKVError(RuntimeError):
    pass
class ServerError(DistKVError):
    pass
class ClientError(DistKVError):
    pass

class ServerClosedError(ServerError):
    pass
class ServerConnectionError(ServerError):
    pass

class ClientAuthError(ClientError):
    pass
class ClientAuthRequiredError(ClientAuthError):
    pass
class ClientAuthMethodError(ClientAuthError):
    pass

class NoAuthModuleError(ClientError):
    pass
