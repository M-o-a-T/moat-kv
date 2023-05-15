=======================
Principles of operation
=======================

MoaT-KV relies on the fact that on most KV storage systems, any given record
is rarely (if ever) changed by more than one entity at the same time. Thus,
a simple gossip protocol is sufficient for distributing data.

To recover from missed changes, each node in a MoaT-KV network maintains a
change counter ("tick"). All data records (:class:`moat.kv.model.Entry`) are
tagged with a chain of events (:class:`moat.kv.model.NodeEvent`), consisting
of the ``n`` most recent ``(node, tick)`` values which changed this
entry. Nodes do not appear in a chain more than once. Dropped ticks
are added to a per-node list of "known"(-to-have-been-superseded) counter
values.

The maximum chain length is determined by the number of partitions a MoaT-KV
network might split into. Thus the network guarantees that it is possible
which side of a split modified a record when the split is healed.

If both sides did, the conflict is resolved deterministically.
TODO: when this happens, send a notification to clients.

After a network split, a four-step protocol re-synchronizes the
participants:

* broadcast the current counters

* broadcast known-value and known-deleted lists

* broadcast a list of missing node events

* broadcast the missed data

MoaT-KV does not have a master node, much less a consensus-based election
system (Raft, Paxos, …). Instead, MoaT-KV uses an ``asyncserf Actor`` to
compile a short list of available servers that's broadcast every few
seconds.

When a partitioned network is re-joined, the current housekeepers are
responsible for driving and monitoring the re-sync protocol.


Storage
=======

MoaT-KV is intended to be used in a mostly-RAM architecture. There is no
disk-based storage backend; snapshots and event logs are used to restore a
system, if necessary. Feeding old snapshots to a running system is mostly
benign, but see below.


MoaT-KV is based on the gossip system provided by Hashicorp's Serf.
It supports all data types that can be transmitted by
`MsgPack <https://github.com/msgpack/msgpack/blob/master/spec.md>`.

TODO: MsgPack has extension types, so constructing Python objects is possible.

Record Deletion
===============

Deleting data records is when MoaT-KV's synchronization protocol breaks
down, because MoaT-KV can't attach chains to records which no longer exist.

MoaT-KV fixes this by keeping a separate record of deleted entries, or
rather their chain links. This works well for mostly-static storages but
becomes a problem on more dynamic systems.

Thus, periodic clean-up is required. This is achieved by creating a
separate "Delete" Actor group which contains every system with persistent
storage plus one system per network that's not already covered.

When every node of this group is online, they periodically broadcast a
tuple of ``tock`` values: one which signals that deletions with earlier
``tock``\s may safely be flushed, and a high-water limit for the next
round.

A node that receives this tuple compares the received first value with the
last transmission's second. If it's higher, deletions may have been missed,
most likely due to a network outage between that node and the closest Delete
member. Since the records are now gone, the node will connect to one of the
Delete group members and send a list of each entry's last-change chain links.
The recipient will re-broadcast any misses as "new" deletions.

