Open issues
===========

* We need path translation. Idea: store an extension element in the
  destination path, which would pick the appropriate parts from the
  source path when processed.

  2-element tuples would probably work also, given that it's unlikely that
  people use complex elements in their path, but why limit ourselves?

* ACLs for system data, i.e. those stored below ``None``.

* after starting with initial data, wait until the Actor is up and we're
  synced to the other nodes

