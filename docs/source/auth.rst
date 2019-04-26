=========================
DistKV and authentication
=========================

DistKV ships with a couple of rudimentary auth modules.

Currently there is no access control. That's on the TODO list.

Included user auth methods
==========================

root
----

No access control. There is one user named "*".

password
--------

Username plus password.

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
