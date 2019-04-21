=========================
DistKV and authentication
=========================

DistKV ships with a couple of rudimentary auth modules.

Currently there is no access control. That's on the TODO list.

Included auth methods
=====================

root
----

No access control. There is one user named "*".

password
--------

Username plus password.

API
===

.. module:: distkv.auth

.. autoclass:: BaseServerUser
   :members:

.. autoclass:: BaseClientUser
   :members:

.. autoclass:: BaseServerUserMaker
   :members:

.. autoclass:: BaseClientUserMaker
   :members:
