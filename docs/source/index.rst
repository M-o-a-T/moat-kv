.. documentation master file, created by
   sphinx-quickstart on Sat Jan 21 19:11:14 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


===============================================
DistKV: A distributed no-master key-value store
===============================================

Rationale
=========

When you look into distributed storage, you quickly run into the CAP
theorem (or Brewer's theorem): you can't get all of Consistency,
Availability and Partition tolerance. The problem is that you do want all
three of these.

One way around this problem is to recognize that on most KV storage
systems, any given record is rarely (if ever) changed by more than one
entity at the same time. Thus, a simple gossip protocol is sufficient
for distributing the data. DistKV is based on the gossip system provided by
Hashicorp's Serf.

DistKV does not have a master node, much less a consensus-based election
system (Raft, Paxos, …). Instead, every few seconds one server broadcasts a
message which contains a list of the last few senders. A pseudo-random
back-off and retry algorithm semi-deterministically selects a new peer.

DistKV takes a novel approach when re-integrating separated nodes
or partitioned networks. Each tick, the dedicated node will select
a random host from the unreachable-member list (which is also gossiped) and
broadcast its intent to reconnect with it. Upon success, the nodes involved
will exchange their host lists, extract which information the other
partition does not have, feed that information into the "other" gossip
subnetwork, and only then reconnect the networks.

DistKV does not support data partitioning. Every node stores the whole
data set.

DistKV is intended to be used in a RAM architecture. There is no disk-based
storage backend; periodic snapshots and event logs are used to restore a
system, if necessary.

API
===

DistKV offers an efficient msgpack-based interface to access data and to
change settings. For compatibility, a front-end that mimics etcd2 is also
available.

Status
======

All of the above is still wishful thinking.

.. toctree::
   :maxdepth: 2

   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
