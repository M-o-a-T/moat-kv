class DistKVError(RuntimeError):
    """Superclass of all DistKV errors.

    Abstract class.
    """

    pass


class ServerError(DistKVError):
    """Generic server error.
    
    This class includes errors forwarded to the client.
    """

    pass


class ClientError(DistKVError):
    """Generic client error.
    
    Abstract class.
    """

    pass


class ServerClosedError(ServerError):
    """The server closed our connection."""

    pass


class ServerConnectionError(ServerError):
    """Some connection error"""

    pass


class CancelledError(ClientError):
    """A client call was cancelled."""

    pass


class ClientAuthError(ClientError):
    """Authorization failed.
    
    Abstract class.
    """

    pass


class ClientAuthRequiredError(ClientAuthError):
    """Authorization required but missing."""

    pass


class ClientAuthMethodError(ClientAuthError):
    """Wrong authorization method provided."""

    pass


class DistKVauthError(ClientError):
    """Auth error.

    Abstract class.
    """

    pass


class NoAuthError(DistKVauthError):
    """Server-side error: auth required"""

    pass


class NoAuthModuleError(DistKVauthError):
    """Server-side error: auth module doesn't exist"""

    pass


class AuthFailedError(DistKVauthError):
    """Server-side error: auth failed"""

    pass
