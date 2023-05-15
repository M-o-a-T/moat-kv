==============================
Fixing MoaT-KV network problems
==============================

As the MoaT-KV network is fully asynchronous, there's no way to avoid
getting into trouble – there's no arbitration of inconsistent data.

This document explains how to get back out, if necessary.

Missing data
============

See the `Server protocol <server_protocol>` for details on how MoaT-KV
works. From that document it's obvious that when a node increments its
``tick`` but the associated data gets lost (e.g. if the node or its Serf
agent crashes), you have a problem.

Worse: a server will not start if the "missing" list is non-empty. The
problem is that stale data causes difficult-to-resolve inconsistencies
when written to. TODO: allow the server to be in maintainer-only mode when
that happens.

First, run ``moat kv internal state -ndmrk``. Your output will look
somewhat like this::

    deleted:  # Ticks known to be deleted
      test1:
      - 12
    known:  # Ticks known to be superseded
      test1:
      - 1
      - - 3
        - 10
      test2:
      - 1
    missing:  # Ticks we need to worry about
      test1:
      - 2
    node: test1  # the server we just asked
    nodes:  # all known nodes and their ticks
      test1: 12
      test2: 1
    remote_missing: {}  # used in recovery
    tock: 82  # MoaT-KV's global event counter
    
This is not healthy: The ``missing`` element contains data. You can
manually mark the offending data as stale::

   one $ moat kv internal mark test1 2
   known:
      test1:
      - - 1
        - 11
      test2:
      - 1
    node: test1
    tock: 92  # If this is not higher than before, clean your glasses ;-)
    one $

This shows that the offending ``tick`` has been successfully added to the
``known`` list. Calling ``moat kv internal state -m`` verifies that
the list is now empty.

Use the ``--broadcast`` flag to send this message to all MoaT-KV servers,
not just the one you're a client of.

This action will allow the bad record to re-surface when the node that has
the record reconnects, assuming that there is one. You can use the ``mark``
command's ``--deleted`` flag to ensure that it will be discarded instead.

