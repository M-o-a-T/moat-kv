=========================
DistKV and authentication
=========================

DistKV ships with a couple of rudimentary auth modules.

Currently there is no access control. That's on the TODO list.

Included user auth methods
==========================

root
----

No access control. There is one possible user named "*".

password
--------

This is the standard "username plus password" method. Passwords are hashed
and salted on the server; transmission of the cleartext password is
protected with a separate shared secret (Diffie-Hellman).

This method currently is a bit slow, unless you use test mode (in which
case it's a bit insecure).

_test
-----

This is a test method that's mostly suitable for experiments. It intentionally
exchanges redundant messages between client and server.

Users do not have a password.


API
===

The authorization code is modular. DistKV allows loading multiple auth
methods, one of which is active. A method may use more than one record type
(think "user" or "group"). Each of those records has a name.

The "user" type is only special because server and client use that to
process login requests.

Multiple distinct DistKV domains or subdomains are possible, by adding an
additional meta-root record anywhere in the entry hierarchy.


.. module:: distkv.auth

.. autofunction:: loader

.. autoclass:: BaseServerAuth
   :members:

.. autoclass:: BaseClientAuth
   :members:

.. autoclass:: BaseServerAuthMaker
   :members:

.. autoclass:: BaseClientAuthMaker
   :members:
