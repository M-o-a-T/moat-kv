"""
This module affords all MoaT-KV exceptions.
"""

# pylint: disable=unnecessary-pass


error_types = {}


def _typed(cls):
    error_types[cls.etype] = cls
    return cls


class MoaT-KVError(RuntimeError):
    """Superclass of all MoaT-KV errors.

    Abstract class.
    """

    pass


class ServerError(MoaT-KVError):
    """Generic server error.

    This class includes errors forwarded to the client.
    """

    pass


class ClientError(MoaT-KVError):
    """Generic client error.

    Abstract class.
    """

    etype: str = None

    pass


@_typed
class ClientChainError(ClientError):
    """The chain you passed in didn't match the entry"""

    etype = "chain"

    pass


@_typed
class ClientConnectionError(ClientError):
    """Some connection error"""

    etype = "conn"

    pass


class ServerClosedError(ServerError):
    """The server closed our connection."""

    pass


class ServerConnectionError(ServerError):
    """Some connection error"""

    pass


class ACLError(ServerError):
    """An ACL did not match"""

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


class MoaT-KVauthError(ClientError):
    """Auth error.

    Abstract class.
    """

    pass


class NoAuthError(MoaT-KVauthError):
    """Server-side error: auth required"""

    pass


class NoAuthModuleError(MoaT-KVauthError):
    """Server-side error: auth module doesn't exist"""

    pass


class AuthFailedError(MoaT-KVauthError):
    """Server-side error: auth failed"""

    pass
