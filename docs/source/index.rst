.. documentation master file, created by
   sphinx-quickstart on Sat Jan 21 19:11:14 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


===============================================
DistKV: A distributed no-master key-value store
===============================================

Rationale
=========

Any kind of distributed storage is subject to the CAP theorem (also called
"Brewer's theorem"): you can't get all of (global) Consistency,
Availability, and Partition tolerance. The problem is that you do want all
three of these.

One way around this problem is to recognize that on most KV storage
systems, any given record is rarely (if ever) changed by more than one
entity at the same time. Thus, a simple gossip protocol is sufficient
for distributing data.

DistKV does not have a master node, much less a consensus-based election
system (Raft, Paxos, …). Instead, DistKV compiles a short list of available
servers that's broadcast every few seconds. The algorithm to select the
next server is deterministic so that all nodes in a network agree which
server is currently responsible for housekeeping.

When a partitioned network is re-joined, these housekeepers connect to each
other and exchange a series of messages, to establish which updates the
other side(s) appears to have missed out on. These are then re-broadcast.

DistKV does not support data partitioning. Every node knows the whole
data set.

The DistKV client library does not support reconnecting. That is
intentional: if the local server ever dies, your client has stale state and
should not continue to run. The clean solution is to wait until the client
is again operational and up-to-date, and then restart the client.

DistKV is intended to be used in a mostly-RAM architecture. There is no
disk-based storage backend; snapshots and event logs are used to restore a
system, if necessary.

DistKV is based on the gossip system provided by Hashicorp's Serf.
It supports all data types that can be transmitted by
`MsgPack <https://github.com/msgpack/msgpack/blob/master/spec.md>`.

TODO: MsgPack has extension types, so constructing Python objects is possible.

API
===

DistKV offers an efficient interface to access and change data. For
compatibility, a front-end that mostly-mimics the etcd2 protocol is
planned.


Status
======

Some of the above is still wishful thinking. In particular, we don't have
an etcd2 compatibility service yet.


.. toctree::
   :maxdepth: 2

   client_protocol.rst
   server_protocol.rst
   auth.rst
   translator.rst
   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
