======
DistKV
======

Welcome to `DistKV <https://github.com/smurfix/distkv>`__!

DistKV is a master-less distributed key-value storage system. It
circumvents the CAP theorem by assuming that keys are usually only changed
by one node. It is resistant to partitioning and intended to be always-on;
while it might delay – but will not lose – any updates.

DistKV does not support data partitioning. Every node stores the whole
data set and can instantly deliver mostly-uptodate data.

DistKV does not have a disk-based storage backend; periodic snapshots and
event logs are used to restore a system, if necessary.

API
===

DistKV offers an efficient msgpack-based interface to access data and to
change settings. For compatibility, a front-end that mimics etcd2 is
available.

Status
======

Some of the above is still wishful thinking.

DistKV has rich accessors and can distribute data, but not yet recover.
