======
DistKV
======

Welcome to `DistKV <https://github.com/smurfix/distkv>`__!

DistKV is a master-less distributed key-value storage system. It
circumvents the CAP theorem by assuming that keys are usually only changed
by one node. It is resistant to partitioning and intended to be always-on;
it might delay – but will not lose – updates even in a partitioned network.

DistKV comes with several batteries included:

* Basic user management, pattern-based ACLs

* Strong typing, code- and/or `JSON Schema`-based

* Data mangling

* Background code execution

* Seamless recovery even if only one master is running

* a MQTT 3.1 back-end that stores persistent data in DistKV,
  based on hbmqtt

API
===

DistKV offers an efficient msgpack-based interface to access data and to
change internal settings. Most configuration is stored inside DistKV
itself.

Stored data are **not** forced to be strings or binary sequences, but can
be anything that `MsgPack` supports. Keys to storage are multi-level.

For compatibility, a front-end that mimics etcd2 is available (TODO).

Non-Features
============

DistKV does not support data partitioning. Every node stores the whole
data set and can instantly deliver mostly-uptodate data.

DistKV does not have a disk-based storage backend; periodic snapshots and
event logs can be used to quickly restore a system, if necessary.

Status
======

DistKV has rich accessors and can distribute data, but not yet recover.
