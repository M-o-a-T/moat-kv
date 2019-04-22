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


class CancelledError(ClientError):
    pass


class ClientAuthError(ClientError):
    pass


class ClientAuthRequiredError(ClientAuthError):
    pass


class ClientAuthMethodError(ClientAuthError):
    pass


class DistKVauthError(ClientError):
    pass


class NoAuthError(DistKVauthError):
    pass


class NoAuthModuleError(DistKVauthError):
    pass


class AuthFailedError(DistKVauthError):
    pass
