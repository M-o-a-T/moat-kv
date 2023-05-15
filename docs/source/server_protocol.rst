========================
MoaT-KV's server protocol
========================

MoaT-KV instances broadcast messages via `Serf <http://serf.io>` or
`MQTT <https://mqtt.org>`.
The payload is encoded with `msgpack
<https://github.com/msgpack/msgpack/blob/master/spec.md>` and sent 
as user events (Serf) / to topics (MQTT) with a configurable prefix.


++++++++++
Data types
++++++++++

Node
++++

A node represents a server that has injected at least one data item into
the DiskKV network. Each node has an associated "tick", which is the
sequence number of the last change that this node injected into the
network. An empty node starts with a counter of zero; the network's first
node starts with 1.

Entry
+++++

An entry is some data stored in MoaT-KV. The entry has a name, or (more
correctly) a path, i.e. a sequence of names leading from the server's root
to the entry. An entry can store one chunk of data, or it can be empty.

Path members may be UTF-8 strings, byte strings, or numbers. The empty
UTF-8 and byte strings are considered equivalent, any other values are not.
If you want to use the MoaT-KV command line to access data, you should limit
yourself to UTF-8 strings.

For ensuring consistency, each entry also has an associated chain, which
documents which node(s) last changed that entry.

Chain
+++++

A chain, in MoaT-KV, is a bounded list of ordered ``(node, tick)`` pairs.

* ``node`` is the node that effected a change.
  
* ``tick`` is a node-specific counter which increments by one when any
  entry on that node is changed.

A chain entry might not have a ``tick`` element. In that case the node has
not been initialized yet. Such entries are only valid in ``ping`` chains.

Chains are governed by three rules:

* The most recent change is at the front of the chain.

* Any node may only appear on the chain once, with the ``tick`` of the
  latest change by that node. If a node changes an entry again, the old
  entry is removed before the new entry is prepended.

  This rule does not apply to ``ping`` chains.

* Their length is bounded. If a new entry causes the chain to grow too
  long, the oldest entry is removed.

Chains are typically represented by ``(node,tick,prev)`` maps, where
``prev`` is either ``None`` (the chain ends here), nonexistent (the chain
was truncated here), or another chain triple (the last previous change on a
different node).

Ticks increment sequentially so that every node can verify that it
knows all of every other node's changes.

The chain concept is based on `vector clocks <https://queue.acm.org/detail.cfm?id=2917756>`.
Nodes are sorted so that causality may be established more easily (no need
to compare the whole vectors) and vector length may be bounded without
sacrificing reliability.

The default chain length should be two larger than the maximum of

* the number of partitions a MoaT-KV system might break up into,
  
* the number of hosts within one partition that might change any single entry.
  Ideally, this number should be two: one for the host that does it as a
  matter of fact, e.g. a measurement system, and one for any manual intercession.

tick
++++

Each node has an associated tick, which is a contiguous counter of changes
by that node. Each value between one and a node's ``tick`` must be
present either in exactly one entry's chain or in that node's ``known``
range.

Tick values are 63-bit unsigned integers. As this space requires 20 mio
years to wrap around, assuming ten messages per millisecond (which is way
above the capacity of a typical Serf network), the MoaT-KV protocol does not
specify what shall happen if this value overflows.

tock
++++

The ``tock`` counter is a system-wide number that's incremented whenever
something interesting happens on a node (most important: some entry is
changed). All messages carry the current ``tock`` value; entries store the
``tock`` from their last change. Whenever a MoaT-KV server receives a
message with a ``tock`` higher than its own, the local ``tock`` is set to
the incoming message's ``tock`` value.

The main purpose of this value is to establish rough temporal consistency
(in the absence of network splits). Secondarily, when a split is healed, 
their ``tock`` value resolves the resulting Actor conflict. (The tie
breaker is the name of the current group leaders.)

Coordination
++++++++++++

Nodes coordinate so that any housekeeping messages are transmitted by
exactly one node instead of flooding the network. This is facilitated by
the ``asyncactor`` module.

When a network split is healed, the Actor protocol notices. It then
triggers ``info`` messages that retrieve any missed changes.

Putting it all together
+++++++++++++++++++++++

Each node has a tick counter that increments when you change anything; it's
also broadcast periodically. Thus, each node notices when there's missing
data and will send a message seeking the missing items.

Each node is associated with a range of ``known`` ticks, which says "yes I
have once seen this message, but it's been superseded".

The entries' ``chain`` data ensures that stale data cannot overwrite more
recent messages.

Deletion of entries
+++++++++++++++++++

The entries' change chains determine that no entry gets lost, but that
mechanism depends on the entries themselves to exist. In a MoaT-KV system
that's highly dynamic, this is undesireable and would cause a lot of stale
entries to accumulate. Removing these entries must be coordinated: if a
removal is lost for any reason, the system cannot recover without manual
intervention.

Therefore, each node also carries a ``deleted`` list which marks entries
that have been cleared. Deleted entries will only be cleared if all nodes
that are on the internal "deleter" list are online.


++++++++++++
Common items
++++++++++++

Bidirectional
+++++++++++++

path
----

The path to the entry you're accessing. This is a list. The contents of
that list may be anything hashable, i.e. strings, integers,
``True``/``False``/``None``.

.. note:

    ``None`` is MoaT-KV's special name for its meta hierarchy, i.e. data
    about itself (user IDs, file conversion code, …). As such it is not
    directly accessible.

value
-----

A node's value. This can be anything that ``msgpack`` can work with: you do
not need to encode your values to binary strings, and in fact you should
not because some of MoaT-KV's features (like type checking) would no longer
work, or be much more awkward to use.

Replies
+++++++

node
----

The node which is responsible for this message. For ``update`` events this
is the node which originated the change; for all other events, it's the
sending node.

tick
----

This node's current tick. The tick is incremented every time a value is changed by that node.

prev
----

A dict with ``node,tick,prev`` entries, which describes the node which
originated the change that is is based on.

If this value is ``None``, the entry has been created at that time. If it
is missing, further chain members have been elided.

In the client protocol, the ``node``, ``tick`` and ``prev`` members are
stored in a ``chain`` element; otherwise the semantics are the same.

A chain will not contain any node more than once. When a value is changed
again, that node's ``tick`` is incremented, its entry is added or moved
to the head of the chain.

tock
----

This is a global message counter. Each server has one; it is incremented
every time its node counter is incremented or a Serf message is sent.
A server must not send a message with a smaller (or equal) ``tock`` value
than any it has received, or previously sent. Since Serf does ot guarantee
order of delivery, receiving a message with a smaller ``tock`` than the
preceding one is not an error.

+++++++++++++
Message types
+++++++++++++

update
++++++

This message updates an entry.

Each server remembers the change chain's per-node ``tick`` values so that
it can verify that all messages from other servers have been received.

path
----

The list of path elements leading to the entry to be updated.

value
-----

The value to set. ``Null`` means the same as deleting the entry.

info
++++

This message contains generic information. It is sent whenever required.

known
-----

This element contains a map of (node ⇒ ranges of tick values) which the
sending server has seen. This includes existing events as well as events
that no longer exist; this happens when a node re-updates an entry.

This message's change chain refers to the ``ping`` it replies to.

ticks
-----

This element contains a map of (node ⇒ last_tick_seen), sent to verify that 

missing
-------

A map of (node ⇒ ranges of tick values) which the sending node has not
seen. Any node that sees this request will re-send change messages in that
range.

reason
------

This element is sent in the first step of split reconciliation recovery. If
the first ``ping`` after being reconnected "wins", then the winning side
needs to be told that there's a problem.

This element contains the losing side's ping chain, which the nodes in the
winning side's ping chain use to initiate their recovery procedure.

ping
++++

A periodic "I am alive" message. This message's change chain shows which
node was pinged previously.

++++++++++++++++++++++
Timing and concurrency
++++++++++++++++++++++

Server to Server
++++++++++++++++

Ping sequence
-------------

Every ``clock`` seconds each node starts thinking about sending a ``ping``
sometime during the next ``clock`` seconds. The node that's last in the
chain (assuming that the chain has maximum length) does this quite early,
while the node that transmitted the previous ``ping`` does this at the end
of the interval. Nodes not in the current chain do this immediately, with
some low probability (one to 10 times the number of known nodes) so that
the chain varies. If no ``ping`` has arrived after another ``clock/2``
seconds, each node sends a ping sometime during the next ``clock/2``
seconds. Thus, at least one ``ping`` must be seen every ``3*clock``
seconds.

Ping messages can collide. If so, the message with the higher ``tock``
value wins. If they match, the node with the higher ``tick`` value wins. If
they match too, the node with the alphabetically-lower name wins. The
winning message becomes the basis for the next cycle.

This protocol assumes that the ``prev`` chains of any colliding ticks are
identical. If they are not, there was at least one network split that is
now healed. When this is detected, the nodes mentioned in the messages'
chains send ``info`` messages containing ``ticks`` for all nodes they know.
The non-topmost nodes will delay this message by ``clock/ping.length``
(times their position in the chain) seconds and not send their message if
they see a previous node's message first. Resolution of which chain is the
"real" one shall proceed as above.

``clock`` is configurable (``ping.clock``); the default is ``5``. It must be at
least twice the time Serf requires to delivers a message to all nodes.

The length of the ping chain is likewise configurable (``ping.length``).
It should be larger than the number of possible network partitions; the
default is 4.

TODO: Currently, this protocol does not tolerate overloaded Serf networks
well, if at all.


Startup
-------

When starting up, a new node sends a ``ping`` query with an empty ``prev``
chain, every ``3*clock`` seconds. The initial ``tick`` value shall be zero;
the first message shall be delayed by a random interval between ``clock/2``
and ``clock`` seconds.

Reception of an initial ``ping`` does trigger an ``info`` message, but does not
affect the regular ``ping`` interval, on nodes that already participate in
the protocol. A new node, however, may assume that the ``ping`` message it
sees is authoritative (unless the "new"  ``ping`` is followed by one with a
non-empty chain). In case of multiple nodes joining a new network, the last
``ping`` seen shall be the next entry in the chain. 

The new node is required to contact a node in the (non-empty) ping chain it
attaches to, in order to download its current set of entries, before
answering client queries. If a new node does already know a (possibly
outdated) set of messages and there is no authoritative chain, it shall
broadcast them in a series of ``update`` messages.

The first node that initiates a new network shall send an ``update`` event
for the root node (with any value). A chain is not authoritative if it only
contains nodes with zero ``tick`` values. Nodes with zero ticks shall not
send a ``ping`` when the first half of the chain does not contain a
non-zero-tick node (unless the second half doesn't contain any such nodes
either).

The practical effect of this is that when a network is restarted,
fast-starting empty nodes will quickly agree on a ``ping`` sequence. A node
with recovered data, which presumably takes longer to start up since it has
to load the data first, will then take over as soon as it is operational;
it will not be booted from the chain by nodes that don't yet have recovered
the data store.


Event recovery
--------------

After a network split is healed, there can be any number of update events
that the "other side" doesn't know about. These need to be redistributed.

Step zero: a ``ping`` message with an incompatible chain arrives.

First step: Send an ``info`` message with a ``ticks`` element, so that any
node that has been restarted knows which tick value they are supposed to
continue with.

Second step (after half a tick): Send a message with ``missing`` elements
that describe which events you do not yet know about.

Third step: Nodes retransmit missing events, followed by a ``known``
message that lists ticks which no longer appear on an event's chain.

After completing this sequence, every node should have a node list which
marks no event as missing. For error recovery, a node may randomly
(at most one such request every ``10*clock`` interval) retransmit its
local ``missing`` list, assuming there is one.

This protocol assumes that new nodes connect to an existing non-split
network. If new nodes first form their own little club before being
reconnected to the "real" network (or a branch of it), this would force a
long list of events to be retransmitted. Therefore, nodes with zero ticks
must initially be passive. They shall open a client connection to any
on-chain node and download its state. If a node has received a non-zero
tick for itself in a ``known`` message, it may participate only after it
has received a complete download, and must not allow client connections
before its list of missing events is empty.

All of these steps are to be performed by the first nodes in the pre-joined
chains. If these messages are not seen after ``clock/2`` seconds (counting
from reception of the ``ping``, ``ticks`` or ``missing`` element that
occured in the previous step), the second node in the chain is required to
send them; the third node will take over after an additional ``clock/4``
interval, and so on. Of course, only messages originating from hosts on the
correct chain shall suppress a node's transmission.

++++++++++++++
Message graphs
++++++++++++++

Yes, I need to visualize (and test) all of this.

TODO.

++++++++++++++++
MsgPack encoding
++++++++++++++++

MoaT-KV encodes its messages with MsgPack. It's fast, compact,
self-delimiting, and easily translated from/to human-readable YAML.

MoaT-KV uses the following MsgPack extensions:

2: big unsigned integer
+++++++++++++++++++++++

MsgPack is limited to 64bit integers. We exceed that: IPv6 network
addresses are longer. Thus, longer unsigned integers are stored in this
extension. Storage is big-endian and required to be minimal, i.e. the first
byte must not be zero. The length must be >8 obviously.

3: Path
+++++++

Distinguishing Path from ``list`` / ``tuple`` makes sense, if only to clean
up YAML output. Thus, paths are stored separately. The extension's content
is the sequence of encoded path elements.

+++++++++++++
YAML encoding
+++++++++++++

MoaT-KV uses clean, "safe" YAML with no frills, resulting in a simple
human-readable data format.

MoaT-KV's YAML supports two extensions: ``!P`` and ``!bin``.

``!P`` marks a `Path`, which makes the resulting YAML more compact and
readable.

``!bin`` encodes binary data as ASCII, i.e. a simple YAML string. YAML's
default is ``base64`` which cannot be easily edited.

