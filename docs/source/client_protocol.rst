========================
MoaT-KV's client protocol
========================

MoaT-KV's native client protocol is based on MsgPack. The client sends
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

This field tells the MoaT-KV server how many change entries to return.
The default is zero. If you want to update a value, retrieve the
original with ``nchain`` set to one. Synchronization between MoaT-KV servers
requires the number of possible partitions plus one, in order to protect
against spurious conflict reports.

path
----

The subset of data affected by the current command.


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

The value of the MoaT-KV entry, assuming one was requested.

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
never contains any one MoaT-KV server more than once.

See the server protocol for a detailed description.

tick
----

The current server's change counter. This field can be used to ensure that
the local server is not restarted with old state.

tock
----

An always-increasing integer that's (supposed to be) shared within the
whole MoaT-KV system. You can use it when you need to reconnect to a server,
to make sure that the system is (mostly) up-to-date.

path
----

The subset of data described by the current message.

depth
-----



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

Sample::

    >>> {'seq': 0, 'version': (0, 58, 12), 'node': 'dev', 'tick': 350225, 'tock': 558759182, 'qlen': 10, 'auth': ('password',)}

auth
----

Tell the server about your identity. This method **must** be sent first, if
the server requests authorization.

The ``identity`` parameter tells the server which user ID (or equivalent)
to use for logging in. ``typ`` contains the auth type to use; this
**must** be identical to the first entry in the ``connect`` reply's
``auth`` parameter.

If this is not the first auth message, the authorization is verified but the
resulting user identity is ignored.

Example::

    >>> {'typ': 'password', 'ident': 'root', 'password': b'[data]', 'action': 'auth', 'seq': 2}
    <<< {'seq': 2}

test_acl
--------

Check whether the given ``path`` is accessible with the given  ``mode``.

The ``acl`` to test may be specified. The user's ACL, if any, is also
tested; the return message's ``access`` element may contain ``False``
(access not allowed), ``True`` (access allowed but no ACL details
available) or the actual ACL characters.

Access will not be granted if you try to check a specific ACL when your
own rights don't include 'a' (for accessing ACLs).

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

Retrieve a single value.

If the value does not exist or has been deleted, you'll get ``None`` back.

Alternately, you can set ``node`` and ``tick``, which returns the entry
that has been set by this event (if the event is still available). The
entry will contain the current value even if the event has set a previous
value.

Example::
    >>> {'path': P('test.one'), 'action': 'get_value', 'seq': 4}
    <<< {'value': 'Two', 'tock': 12345, 'seq': 4}

set_value
---------

Set a single value. The ``path`` to that ``value`` needs to be sent as a list.

If you are updating a known value, you should send a ``chain`` entry
to help ensure that no other node has changed it unexpectedly. (Of course,
due to the distributed nature of MoaT-KV, this may happen anyway.) You can
also use ``prev`` to send an expected old value, but you really shouldn't.

This action returns the node's new change ``chain``. If you did not send a
``chain`` field, the previous value is returned in ``prev``.

Simple example::

    >>> {'path': P('test.one'), 'value': 'Three', 'action': 'set_value', 'seq': 5}
    <<< {'changed': True, 'tock': 12348, 'seq': 5}

However, this is not particularly safe if you want to modify a value, as
there's no way to ascertain that it hasn't been changed by somebody else in
the meantime. It's safer to retrieve the entry's change log, or at least
its first couple of entries, and then send that along with the
``set_value`` request::

    >>> {'path': P('test.one'), 'nchain': 3, 'action': 'get_value', 'seq': 4}
    <<< {'value': 'Three', 'chain': {'node': 'r-a', 'tick': 121, 'prev': None}, 'tock': 12355, 'seq': 4}
    >>> {'path': P('test.one'), 'value': 'Four', 'nchain': 3, 'chain': {'node': 'r-a', 'tick': 121, 'prev': None}, 'action': 'set_value', 'seq': 5}
    <<< {'changed': True, 'chain': {'node': 'dev', 'tick': 69, 'prev': {'node': 'r-a', 'tick': 121, 'prev': None}}, 'tock': 12358, 'seq': 5}

The ``chain`` value should be treated as opaque, except for``None`` which
indicates that the node doesn't exist.

delete_value
------------

Remove a single value. This is the same as setting it to ``None``. The
``chain`` semantics of ``set_value`` apply.

get_state
---------

Retrieve the current system state. The following ``bool``-valued attributes
may be set to specify what is returned. The corresponding reply is stored
in an attribute of the same name.

All of these data is mainly useful for debugging the replication / recovery
protocol. The resulting lists can become somewhat long on a busy system.

* nodes

A dict of server ⇒ tick. Each server's known Tick values must be
consecutive; when they are not, MoaT-KV tries to retrieve the missing
entries.

* deleted

A dict of server ⇒ ranges of ticks known to have been deleted.

* known

A dict of server ⇒ ranges of ticks known. This contains current data as well
as events that have been superseded.

* current

A dict of server ⇒ ranges of ticks corresponding to the current state of
nodes. This is expensive to calculate. It is a superset of `'known``.

* missing

A dict of server ⇒ ranges of ticks not available on the server. This list is
empty if the server thinks it is up-to-date.

* remote_missing

A dict of server ⇒ ranges of ticks reported to be missing at some other node.

* present

A dict of server ⇒ ranges of entries that actually exist.

* superseded

A dict of server ⇒ ranges of entries that have been replaced by a newer
version of the corresponding node.

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

* min_depth

  Start reporting nodes at this depth.

* max_depth

  Limit recursion depth.

* empty

  Include empty nodes. This is useful when limiting the depth to non-leaf
  nodes without data.

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

msg_send
--------

Pass-through call to transmit a message. Parameters are ``type`` (the user
event to send to) and ``data`` (the data to send).

Raw binary data may be transmitted by using ``raw`` instead of ``data``.

msg_monitor
-----------

Pass-through call to receive brodcast messages. You'll get a
stream with ``data`` containing the decoded message. If decoding fails,
``raw`` contains the message's bytes and ``error`` holds a string
representation of the decoder problem.

Set ``raw`` to True if the incoming messages are not supposed to be
msgpack-encoded in the first place. In this case, ``data`` and ``error``
will always be missing.

Examples
========

You can turn on message debugging with 'moat -vvv kv'.

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

Retrieving this tree with ``moat kv test get -rd ':val'`` would print::

    test:
      :val: 1
      foo:
        :val: 1
        bap: {':val': 12}
        bar:
          :val: 1
          baz: {':val': 1}


