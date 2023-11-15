=========================
MoaT-KV and authentication
=========================

MoaT-KV ships with a couple of rudimentary auth modules.

The server's initial message lists the accepted authentication methods
(``auth`` entry).

Depending on the server version, auth requests may be answered with a
stream even if the method doesn't actually require it. Login is successful
if the reply (or stream-end message) doesn't contain an error.

Included user auth methods
==========================

root
----

No access control. There is one possible user named "*"::

    <<< {'seq': 0, 'version': (0, 58, 12), 'node': 'dev', 'auth': ('root',), …}
    >>> {'typ': 'root', 'ident': '*', 'action': 'auth', 'seq': 1}
    <<< {'state': 'start', 'seq': 2, 'wseq': 1, 'tock': 123}
    <<< {'state': 'end', 'seq': 2, 'wseq': 2, 'tock': 124}

password
--------

This is the standard "username plus password" method. Passwords are hashed
and salted on the server; transmission of the cleartext password is
protected with a separate shared secret (Diffie-Hellman).

This method currently is a bit slow, unless you use test mode (in which
case it's a bit insecure).

The client initiates a Diffie-Hellman handshake if required, then wraps the
SHA256 of the password in a ``SecretBox`` (using a random nonce) and sends
that to the server. Logging in as ``root``::

    <<< {'seq': 0, 'version': (0, 58, 12), 'node': 'dev', 'auth': ('password',), …}
    >>> {'pubkey': b'[256 bytes]', 'length': 1024, 'action': 'diffie_hellman', 'seq': 1}
    <<< {'pubkey': b'[256 bytes]', 'seq': 1, 'tock': 999}
    >>> {'typ': 'password', 'ident': 'root', 'password': b'[data]', 'action': 'auth', 'seq': 2}
    <<< {'state': 'start', 'seq': 2, 'wseq': 1, 'tock': 1001}
    <<< {'state': 'end', 'seq': 2, 'wseq': 2, 'tock': 1002}

_test
-----

This is a test method that's suitable for experiments and testing.

Users do not have a password.


API
===

The authorization code is modular. MoaT-KV allows loading multiple auth
methods, one of which is active. A method may use more than one record type
(think "user" or "group"). Each of those records has a name.

The "user" type is only special because server and client use that to
process login requests.

Multiple distinct MoaT-KV domains or subdomains are possible, by adding an
additional meta-root record anywhere in the entry hierarchy.


.. module:: moat.kv.auth

.. autofunction:: loader

.. autoclass:: BaseServerAuth
   :members:

.. autoclass:: BaseClientAuth
   :members:

.. autoclass:: BaseServerAuthMaker
   :members:

.. autoclass:: BaseClientAuthMaker
   :members:
