======
MoaT-KV
======

Welcome to `MoaT-KV <https://github.com/smurfix/moat.kv>`__!

MoaT-KV is a master-less distributed key-value storage system. It
circumvents the CAP theorem by assuming that keys are usually only changed
by one node. It is resistant to partitioning and intended to be always-on;
it might delay – but will not lose – updates even in a partitioned network.

MoaT-KV comes with several batteries included:

* Basic user management, pattern-based ACLs

* Strong typing, code- and/or `JSON Schema`-based

* Data mangling

* Background code execution

* Seamless recovery even if only one master is running

* a MQTT 3.1 back-end that stores persistent data in MoaT-KV,
  based on hbmqtt

API
===

MoaT-KV offers an efficient msgpack-based interface to access data and to
change internal settings. Most configuration is stored inside MoaT-KV
itself.

Stored data are **not** forced to be strings or binary sequences, but can
be anything that `MsgPack` supports. Keys to storage are multi-level and
support string, integer/float, and list keys.


Non-Features
============

MoaT-KV does not support data partitioning. Every node stores the whole
data set and can instantly deliver mostly-uptodate data.

MoaT-KV does not have a disk-based storage backend; periodic snapshots and
event logs can be used to quickly restore a system, if necessary.

Status
======

MoaT-KV is mostly stable. There are a lot of corner cases that don't
have tests yet

TODOs:
* some services (esp. command line tools and runners) are under-tested
* there's no good API for errors

Changelog
=========

0.41: the message monitor can do multiple subpaths and only reports initial-load-complete once

0.40: use asyncscope for running subsystems in a reasonable way

0.35: allow forgetting nodes (if they have no data attached)

0.30: major API refactoring: paths are now separate objects

TODO
====

* update the whole ecosystem to anyio 2.0 (asyncclick asyncscope …)

* clean up some of the more egregious command line mistakes

* create a page for showcase-ing subprojects (distinv knx owfs akumuli …)

* improve Home Assistant integration

