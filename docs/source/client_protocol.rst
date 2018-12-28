========================
DistKV's client protocol
========================

DistKV's native client protocol is based on MsgPack. The client sends
requests; the server sends one or more responses. You may (and indeed
should) run concurrent requests on the same connection.

Strings must be UTF-8, as per MsgPack specification.

Requests and replies are mappings.

The server initially sends a greeting, using sequence number zero. It will
not send any other unsolicited message.

Requests
========

Client requests are always mappings. ``seq`` and ``action`` must be
present. All other fields are request specific. The server will ignore
fields it doesn't understand.

seq
---

Every client request must contain a strictly increasing positive sequence
number. All replies associated with a request carry the same sequence
number.

action
------

The action which the server is requested to perform. Valid actions are
described below.

nchain
------

This field tells the DistKV server how many change entries to return.
The default is zero. If you want to update a value, retrieve the
original with ``nchain`` set to one. Synchronization between DistKV servers
requires the number of possible partitions plus one, in order to protect
against spurious conflict reports.


Replies
=======

Server replies are always mappings. At least one of ``seq`` and ``error``
must be present. The client must ignore fields it doesn't expect.

seq
---

The sequence number of the request which caused this reply.

Server messages which don't include a sequence number are errors and
will close the connection.

The server will either send exactly one reply with any given sequence number,
or a multi-reply sequence which starets with a ``state=start`` message.

error
-----

This field contains a human-readable error message. A request has failed if
this field is present.

value
-----

The value of the DistKV entry, assuming one was requested.

state
-----
May be ``start`` or ``end``

* ``start`` indicates the beginning of a multi-value result.

* ``end`` indicates that a multi-value result has finished. No more
  messages with this sequence number will be sent.

The ``start`` message will contain neither an error nor a value.

chain
-----

The change chain resulting from, or retrieved by, a command.

Change chains track which server last modified a value, so that replayed
updates can be ignored and conflicting updates can be recognized. A chain
never contains any one DistKV server more than once.


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

Retrieve a single value. The ``path`` to the value needs to be sent as a list.

If the value does not exist or has been deleted, you'll get ``None`` back.

set_value
---------

Set a single value. The ``path`` to that ``value`` needs to be sent as a list.

If you are updating a known value, you should send a ``chain`` entry
to help ensure that no other node has changed it unexpectedly. (Of course,
due to the distributed nature of DistKV, this may happen anyway.) You can
also use ``prev`` to send an expected old value, but you really shouldn't.

This action returns the node's new change ``chain``. If you did not send a
``chain`` field, the previous value is returned in ``prev``.

delete_value
------------

Remove a single value. This is the same as setting it to ``None``.

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

watch
-----

Stream changes to this node. The replies look like those from ``get_tree``.

The recommended way to use this is to first open a monitor and then fill in
unknown values via ``get_values``. This way you won't lose any changes.

Examples
========

You can turn on message debugging with 'distkv -vvv'.

Get and set a value
-------------------

If the value is not set::

    Send {'path': ('test',), 'nchain': 3, 'action': 'get_tree', 'seq': 1}
    Recv {'value': None, 'seq': 1}

Setting an initial value::

    Send {'value': 1234, 'path': ('test',), 'nchain': 2, 'chain': None, 'action': 'set_value', 'seq': 2}
    Recv {'changed': True, 'chain': {'node': 'test1', 'tick': 2, 'prev': None}, 'seq': 2}

Trying the same thing again will result in an error::

    Send {'value': 1234, 'path': ('test',), 'nchain': 2, 'chain': None, 'action': 'set_value', 'seq': 3}
    Recv {'error': 'This entry already exists', 'seq': 3}

To fix that, use the chain value you got when setting or retrieving the
previous value::

    Send {'value': 123, 'path': ('test',), 'nchain': 2, 'chain': {'node': 'test1', 'tick': 2}, 'action': 'set_value', 'seq': 4}
    Recv {'changed': True, 'chain': {'node': 'test1', 'tick': 3, 'prev': None}, 'seq': 4}

Sending no precondition would also work

After you set multiple values::

    Send {'value': 123, 'path': ('test', 'foo'), 'nchain': 0, 'action': 'set_value', 'seq': 5}
    Recv {'changed': True, 'prev': None, 'seq': 5}
    Send {'value': 12, 'path': ('test', 'foo', 'bap'), 'nchain': 0, 'action': 'set_value', 'seq': 6}
    Recv {'changed': True, 'prev': None, 'seq': 6}
    Send {'value': 1, 'path': ('test', 'foo', 'bar', 'baz'), 'nchain': 0, 'action': 'set_value', 'seq': 7}
    Recv {'changed': True, 'prev': None, 'seq': 7}
    Send {'value': 1234, 'path': ('test',), 'nchain': 0, 'action': 'set_value', 'seq': 8}
    Recv {'changed': True, 'prev': 123, 'seq': 8}

you can retrieve the whole subtree::

    Send {'path': ('test',), 'nchain': 0, 'action': 'get_tree', 'seq': 1}
    Recv {'seq': 1, 'state': 'start'}
    Recv {'value': 1234, 'depth': 0, 'seq': 1}
    Recv {'value': 123, 'path': ('foo',), 'depth': 0, 'seq': 1}
    Recv {'value': 12, 'path': ('bap',), 'depth': 1, 'seq': 1}
    Recv {'value': 1, 'path': ('bar', 'baz'), 'depth': 1, 'seq': 1}
    Recv {'seq': 1, 'state': 'end'}

Retrieving this tree with ``distkv client get -ryd ':val' test`` would print::

    test:
      :val: 1
      foo:
        :val: 1
        bap: {':val': 12}
        bar:
          :val: 1
          baz: {':val': 1}


