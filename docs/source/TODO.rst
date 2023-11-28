Open issues
===========

* Exchange a version code on startup

* CBOR

* Ping: ignore messages with decreasing tock (per node)

* chroot operation: add and test proper sub-roots, including auth and
  whatnot

* We need path translation. Idea: store an extension element in the
  destination path, which would pick the appropriate parts from the
  source path when processed.

  2-element tuples would probably work also, given that it's unlikely that
  people use complex elements in their path, but why limit ourselves?

* ACLs for system data, i.e. those stored below ``None``.

* after starting with initial data, wait until the Actor is up and we're
  synced to the other nodes

* Teach the server to also run an executor (or two or three or …)

* Rather than mangling split messages, use a MsgPack extension type.

* AnyRunner: Do proper load balancing; the leader should be able to tell
  some other node to run a job if it's busy.

* Keep an error index on the server?  Something more general?

* Restart code that's been changed (without waiting for restart/retry).

* Use cryptography.hazmat.primitives.asymmetric.x25519 instead of
  Diffie-Hellman to send passwords to the server.

* Implement a shared secret to sign server-to-server messages.

* Runner: switch to monotonic time (except for target time!)

* Error consolidation: if a conflict doesn't get resolved on its own, do it
  anyway when we are "it" next and >1 cycle has passed

* Add a command to cleanly flush the server log and stop the server.

* Test iterator on changed config entries

* errors: better display

* errors: manually acknowledge and delete them

* errors: add a web service to monitor them?

* Runner: store the number of active group members / actor config in the group

* Restore passing positional parameters as keywords (to code entries)

* Add a maintainer mode (user flag) that allows limited access when data is missing

