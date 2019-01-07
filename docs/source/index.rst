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
servers that's broadcast every few seconds. A pseudo-random back-off and
retry algorithm deterministically selects a new peer.

When a partitioned network is re-joined, the servers which currently are on
that list exchange a series of messages to establish which updates the
other side(s) appears to have missed out on. These are then re-broadcast.

TODO::

    DistKV takes a novel approach when re-integrating separated nodes
    or partitioned networks. Each tick, the dedicated node will select
    a random host from the unreachable-member list, and broadcast its intent to
    reconnect with it. Upon success, the nodes involved will exchange their
    host lists, extract which information the other partition does not have,
    feed that information into the "other" gossip subnetwork, and only then
    reconnect the networks.

    This does not work yet, because Serf doesn't support *not* trying to
    auto-reconnect to its peers in a partitioned-off network.

DistKV does not support data partitioning. Every node knows the whole
data set.

The DistKV client library does not support reconnecting. That is
intentional: if the server ever dies, your client and all services it
exports should also terminate (or be terminated).

DistKV is intended to be used in a mostly-RAM architecture. There is no
disk-based storage backend; snapshots and event logs are used to restore a
system, if necessary.

DistKV is based on the gossip system provided by Hashicorp's Serf.
It supports all data types that can be transmitted by
`MsgPack <https://github.com/msgpack/msgpack/blob/master/spec.md>`.

TODO: extend MsgPack with more data types.

API
===

DistKV offers an efficient msgpack-based interface to access data and to
change settings. For compatibility, a front-end that mimics etcd2 is also
available.

Status
======

Some of the above is still wishful thinking. In particular, we don't have
an etcd2 compatibility server yet.


.. toctree::
   :maxdepth: 2

   getting_started.rst
   client_api.rst
   command_line.rst
   client_protocol.rst
   server_protocol.rst
   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
