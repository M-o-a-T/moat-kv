========================
DistKV's client protocol
========================

DistKV's native client protocol is based on MsgPack.

Strings must be UTF-8, as per MsgPack specification.

Requests and replies are mappings.

The server initially sends a greeting, using sequence number zero.

Requests
========

Client requests are always mappings. ``seq`` and ``action`` must be
present. All other fields are request specific. The server will ignore
fields it doesn't understand.

seq
---

Every client message must contain a strictly increasing positive sequence
number.

action
------

The action which the server is requested to perform. Valid actions are
described below.

Replies
=======

Server replies are always mappings. At least one of ``seq`` and ``error``
must be present. The client must ignore fields it doesn't expect.

seq
---

The sequence number of the request which caused this reply.

Server messages which don't include a sequence number are errors and
will close the connection.

error
-----

This field contains a human-readable error message. A request has failed if
this field is present.

res
---

The result of the request, assuming it fits in one message. The server will
not send more replies with this sequence number unless it has first sent
a ``state=start`` message.

state
-----
May be ``start`` or ``end``

* ``start`` indicates the beginning of a multi-value result.

* ``end`` indicates that a multi-value result has finished. No more
  messages with this sequence number will be sent.

Actions
=======

connect
-------

This is a pseudo-action with sequence number zero which the server assumes
to have received after connecting. The server's first message will contain
``seq=0``, its ``node`` name, a ``version`` (as a list of integers), and
possibly its current ``local`` and ``global`` sequence numbers.

stop
----

Send this action to abort a running multi-value request. Set ``task`` to
the sequence number of the request to abort.

This action only works after you received a ``start`` state message.
It returns a :cls:`bool` which is :const:`True` if the command was still
running.

A positive reply does not indicate that no more messages with the stated
sequence number will arrive; this will be indicated by the ``state=end``
message.

get_value
---------

Retrieve a single value. The ``path`` to that value needs to be sent as a list.

If the value does not exist or has been deleted, you'll get ``None`` back.

set_value
---------

Set a single value. The ``path`` to that ``value`` needs to be sent as a list.

This action returns the node's new change chain (possibly truncated).

delete_value
------------

Remove a single value. Currently this is the same as setting it to ``None``.

This action returns the node's new change chain (possibly truncated).

get_tree
--------

Retrieves all values with the prefix given in ``path``.

This is a multi-value reply; each reply contains ``path`` and ``value``
entries. Deleted nodes may or may not be reported.

If the path does not exist or does not have children, a single-value reply
is returned.

Optimization: if a reply contains a "depth" key, its path is shortened by
the request's path, plus that many elements from the previous reply's path.

Thus, if you request a path of ``['a','b','c']``, this reply::

    { seq=13, path=['a','b','c'], value="one" }
    { seq=13, path=['a','b','c','d','e'], value="two" }
    { seq=13, path=['a','b','c','d','f'], value="three" }

is equivalent to::

    { seq=13, depth=0, value="one" }
    { seq=13, depth=0, path=['d','e'], value="two" }
    { seq=13, depth=1, path=['f'], value="three" }

root
----

Switch the client's root to the given path. This request returns the new
root node.

It is not possible to undo this request (other than to reconnect).
Tasks started before this action are not affected.

This action returns the new root node's value.


