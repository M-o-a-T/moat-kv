Open issues
===========

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

* Rate limiting. Currently the server can flood the client with data and no
  other call can get a word in edgewise.

* AnyRunner: Do proper load balancing; the leader should be able to tell
  some other node to run a job if it's busy.

* Keep an error index on the server?  Something more general?

