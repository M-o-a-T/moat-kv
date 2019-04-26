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

See the server protocol for a detailed description.

tick
----

The current server's change counter. This field can be used to ensure that
the local server is not restarted with old state.

tock
----

An always-increasing integer that's (supposed to be) shared within the
whole DistKV system. You can use it when you need to reconnect to a server,
to make sure that the system is (mostly) up-to-date.

Actions
=======

connect
-------

This is a pseudo-action with sequence number zero, which the server assumes
to have received after connecting. The server's first message will contain
``seq=0``, its ``node`` name, a ``version`` (as a list of integers), and
possibly its current ``tick`` and ``tock`` sequence numbers.

The ``auth`` parameter, if present, carries a list of configured
authorization methods. The first method in the list **should** be used to
authorize the client. If the list's first entry is ``None`` then
authorization is not required. Other entries **may** be used for
testing after a client is logged in.

auth
----

Tell the server about your identity. This method **must** be sent first if
the server requests authorization.

The ``identity`` parameter tells the server which user ID (or equivalent)
to use for logging in. ``typ`` contains the auth type to use; this
**must** be identical to the first entry in the ``connect`` reply's
``auth`` parameter.

If this is not the first message, the authorization is verified but the
resulting user identity is ignored.

stop
----

Send this action to abort a running multi-value request. Set ``task`` to
the sequence number of the request to abort.

This action only works after you received a ``start`` state message.
It returns a :class:`bool` which is ``True`` if the command was still
running.

A positive reply does not indicate that no more messages with the stated
sequence number will arrive; this will be indicated by the ``state=end``
message.

get_value
---------

Retrieve a single value. The ``path`` to the value needs to be sent as a list.

If the value does not exist or has been deleted, you'll get ``None`` back.

Alternately, you can set ``node`` and ``tick``, which returns the entry
that has been set by this event (if the event is still available). The
entry will contain the current value even if the event has set a previous
value.

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

get_state
---------

Retrieve the current system state. The following ``bool`` attributes can be
set to specify what is returned. The reply is stored in an attribute of the
same name.

* nodes

A dict of node ⇒ tick.

* known

A dict of node ⇒ ranges of ticks known. This contains current data as well
as events that have been superseded.

* current

A dict of node ⇒ ranges of ticks corresponding to the current state of
nodes. This is expensive to calculate. It is a superset of `'known``.

* missing

A dict of node ⇒ ranges of ticks not available locally. This is the inverse
of ``known``.

* remote_missing

A dict of node ⇒ ranges of ticks reported to be missing at some other node.

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

Monitor changes to this node (and those below it). Replies look like those from ``get_tree``.

The recommended way to run the ``watch`` call with ``fetch=True``. This
fetches the current state and guarantees that no updates are lost. To mark
the end of the static data, the server sends a ``state=uptodate`` message.
This process will not send stale data after an update, so your code may
safely replace an old entry's state with new data.

This task obeys ``min_depth`` and ``max_depth`` restrictions.

save
----

Instruct the server to save its state to the given ``path`` (a string with
a filename).

log
---

Instruct the server to continuously write change entries to the given ``path``
(a string with a filename). If ``fetch`` is ``True``, the server will also
write its current state to that file.

This command returns after the new file has been opened and the initial
state has been written, if so requested. If there was an old log stream,
there may be some duplicate entries. No updates are skipped.

serfsend
--------

Pass-through call to transmit a message via ``serf``. Parameters are
``type`` (the user event to send to), ``data`` (the data to send) and
optionally ``tag`` (a string that limits recipients to Serf nodes with this
tag).

Raw binary data may be transmitted by using ``raw`` instead of ``data``.

serfmon
-------

Pass-through call to receive brodcast messages via ``serf``. You'll get a
stream with ``data`` containing the decoded message. If decoding fails,
``raw`` contains the message's bytes and ``error`` holds a string
representation of the decoder problem.

Set ``raw`` to True if the incoming messages are not supposed to be
msgpack-encoded in the first place. In this case, ``data`` and ``error``
will always be missing.

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


