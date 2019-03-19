========================
DistKV and authorization
========================

Current state: there isn't any.

Intended state: client-server, disk-storage, and server-server
communications are protected with strong cryptography and passwords.

Note that inter-server communication uses Serf, which already has strong crypto
layer. Thus DistKV will not further encrypt its transport.

However, DistKV is agnostic about its payloads. Thus, both keys and values
may pass through an encryption layer.

Configuration storage
---------------------

The DistKV core does not restrict which data types may be stored anywhere.
However, storing configuration and authorization data directly in DistKV
makes sense.

DistKV uses a special namespace below its root to store global
configuration and authorization data, below a ``None`` key.
Ordinary requests may not contain these.


Client-server
=============


