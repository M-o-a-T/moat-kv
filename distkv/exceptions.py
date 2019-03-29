class DistKVError(RuntimeError):
    pass
class ClientError(DistKVError):
    pass
class ClientAuthError(ClientError):
    pass
class ClientAuthRequiredError(ClientAuthError):
    pass
class ClientAuthMethodError(ClientAuthError):
    pass
