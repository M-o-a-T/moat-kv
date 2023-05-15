=================
MoaT-KV's protocol
=================

MoaT-KV's native protocol (both client/server and server/server) is based on
MsgPack. Strings must be valid UTF-8 and are distinct from binary buffers.

++++++++++++++++++
MsgPack Extensions
++++++++++++++++++

MoaT-KV is expected to be a transparent protocol. Unknown extensions
must not cause the reader to crash, and should be round-trip-safe: a client
which reads an object and modifies attribute A should not modify attribute
B even if B contains an element with an unknown extension.

This should, but currently does not, apply to extensions 2 and 3.

--------
DateTime
--------

A client must support MsgPack's Timestamp extension.

------
Bignum
------

Extension 2 is used for unsigned arbitrary-sized integers.

-----
Paths
-----

Extension 3 packages a MsgPack path.

MoaT-KV uses Path objects to refer to its nodes. Paths are lists which may
contain arbitrary msgpack data structures but should be limited to UTF-8
strings and non-negative integers.

A client must support both Path objects and plain lists when reading. It
should send Path elements. Using plain lists instead is supported but
not recommended.

In this documentation's protocol examples, Paths are shown as ``P('some.string')``
for readability.

