==============
Code in DistKV
==============

DistKV can store Python code and modules, either for direct use by your
client or for a runner daemon.

TODO: There is no dependency resolution. Thus, while you can store Python
modules in DistKV, there's no guarantee yet that they'll actually be present
when your code loads.


++++
Code
++++

Python code stored in DistKV is wrapped with a procedure context, mainly to
make returning a result more straightforward. This is done by indenting the
code before compiling it: don't depend on multi-line strings to be flush
left.

Storage
=======

The location for executable scripts is configurable and defaults to
".distkv code proc". Scripts are stored as a dict with these attributes:

* ``code``: the actual text

* ``is_async``: a flag whether the procedure is synchronous (``None``),
  sync but should run in a worker thread (``False``), or async (``True``).

* ``vars``: Input parameters of your procedure. Parameters not mentioned
  here are available in a ``kw`` dict.

* ``requires``: modules which this code needs.  XXX TODO

There's no way for code written for a specific async library to run under
another, with the possible exception of "asyncio top of Trio" (via
``trio-asyncio``. DistKV itself uses ``anyio`` in order to avoid this
poroblem. The author strongly recommends to follow this practice if at all
possible.

The required modules must be stored in DistKV. Accessing modules from your
Python installation or the virtualenv you've set up for DistKV is of course
possible, but DistKV does not try to keep them up-to-date for you.

Call ``cr = await CodeRoot.as_handler(client)``. Then, call some code by
simply naming it: ``cr("forty.two")`` will run the code stored at ``.distkv
code proc forty two``.


+++++++
Modules
+++++++

Python modules are stored to DistKV as plain code.

Recursive dependencies are not allowed.

Storage
=======

The location for Python modules is configurable and defaults to
".distkv code module". Modules are stored as a dict with these attributes:

* ``code``: the actual program text

* ``requires``: other modules which this module needs to be loaded.

Usage
=====

Call ``await ModuleRoot.as_handler(client)``. All modules in your DistKV
store are loaded into the Python interpreter; use normal import statements
to access them.

TODO: Modules are not yet loaded incrementally.


+++++++
Runners
+++++++

The distributed nature of DistKV lends itself to running arbitrary code on
any node that can accomodate it. 

DistKV has two built-in types of code runners.

==================
Single-node runner
==================

This runner executes code on a specific node. This is useful e.g. if you
need to access actual hardware.

In order to be able to implement "emergency operation when disconnected" or
similar fallback strategies, single-node runners can be configured to
provide an Actor group that represents the "central" nodes. Your code can
access a queue that reports whether the central group is partially or
completely (in)visible.

===============
Any-node runner
===============

This runner executes code on some node, largely determined by chance,
startup order, or phase of the moon.

TODO: Load balancing is not yet implemented.

All nodes in a runner form an Actor group; the node that holds the Tag
checks whether jobs need to start.

====================
Runner configuration
====================

Runner entries don't hold code, they merely point to it.

See :class:`distkv.runner.RunnerEntry` for details.

The actcual runtime information is stored in a separate "state" node, mainly to avoid race conditions.
See :class:`distkv.runner.StateEntry` for details.

