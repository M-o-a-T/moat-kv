==============
Code in MoaT-KV
==============

MoaT-KV can store Python code and modules, either for direct use by your
client or for a runner daemon.

TODO: There is no dependency resolution. Thus, while you can store Python
modules in MoaT-KV, there's no guarantee yet that they'll actually be present
when your code loads.


++++
Code
++++

Python code stored in MoaT-KV is wrapped with a procedure context, mainly to
make returning a result more straightforward. This is done by indenting the
code before compiling it: don't depend on multi-line strings to be flush
left.

Storage
=======

The location for executable scripts is configurable and defaults to
":.moat.kv.code.proc". Scripts are stored as a dict with these attributes:

* ``code``: the actual text

* ``is_async``: a flag whether the procedure is synchronous (``None``),
  sync but should run in a worker thread (``False``), or async (``True``).

* ``vars``: Required input variables of your procedure. Parameters not
  mentioned here are still available as globals.

* ``requires``: modules which this code needs.  XXX TODO

There's no way for code written for a specific async library to run under
another, with the possible exception of "asyncio on top of Trio" (via
``trio-asyncio``). MoaT-KV itself uses ``anyio`` in order to avoid the
problem. The author strongly recommends to follow this practice, if at all
possible.

The required modules must be stored in MoaT-KV. Accessing modules from your
Python installation or the virtualenv you've set up for MoaT-KV is of course
possible, but MoaT-KV does not try to keep them up-to-date for you.

If you want to run user code in your MoaT-KV module, call
``cr = await CodeRoot.as_handler(client)``. Then, run some code by
simply naming it: ``cr("forty.two")`` or ``cr(("forty","two"))`` will run
the code stored at ``:.moat.kv.code.proc.forty.two``. All arguments will be
passed to the stored code.


+++++++
Modules
+++++++

Python modules are stored to MoaT-KV as plain code.

Recursive dependencies are not allowed.

Storage
=======

The location for Python modules is configurable and defaults to
":.moat.kv.code.module". Modules are stored as a dict with these attributes:

* ``code``: the actual program text

* ``requires``: other modules which this module needs to be loaded.

Usage
=====

Call ``await ModuleRoot.as_handler(client)``. All modules in your MoaT-KV
store are loaded into the Python interpreter; use normal import statements
to access them.

TODO: Modules are not yet loaded incrementally.


+++++++
Runners
+++++++

The distributed nature of MoaT-KV lends itself to running arbitrary code on
any node that can accomodate it. 

============
Runner types
============

MoaT-KV has three built-in types of code runners. All are organized by a "group"
tag. The "moat kv run all" command starts all jobs of a type, in a
specific group.

``moat kv run`` accepts a ``-g ‹group›`` option that tells the
system which group to use. If you don't use this option, the default group
is named ``default``.

All groups and all runners are distinct. Which nodes actually execute the
code you enter into MoaT-KV is determined solely by running ``moat kv 
run all`` on them, with the appropriate options.

++++++++++++++++++
Single-node runner
++++++++++++++++++

This runner executes code on a specific node. This is useful e.g. if you
need to access non-redundant hardware, e.g. a 1wire bus connected to a
specific computer.

On the command line you access this runner with ``moat kv run -n
NAME``.


+++++++++++++++
Any-node runner
+++++++++++++++

This runner executes code on one of a group of nodes. Which node executes
the code is largely determined by chance, startup order, or phase of the
moon. This is useful when accessing redundant hardware, e.g. a radio
interface.

TODO: Load balancing is not yet implemented.

On the command line you access this runner with ``moat kv run``, i.e.
without using the ``-n ‹node›`` option.

+++++++++++++++
All-node runner
+++++++++++++++

This runner executes code on all members of a group of nodes. You access it
with ``moat kv run -n -``.

====================
Runner configuration
====================

Runner entries don't hold code; they merely point to it. The advantage is
that you can execute the same code with different parameters.

See :class:`moat.kv.runner.RunnerEntry` for details.

The actual runtime information is stored in a separate "state" node.
This avoids race conditions.
See :class:`moat.kv.runner.StateEntry` for details.

+++++++++
Variables
+++++++++

The runners pass a couple of variables to their code.

* _client

  The MoaT-KV client instance. You can use it to access arbitraty MoaT-KV
  data.

* _cfg

  The current configuration.

* _cls

  A dict (actually, `moat.kv.util.attrdict`) with various runner-related
  message classes. Convenient if you want to avoid a cumbersome ``import``
  statement in your code, since these are not part of MoaT-KV's public API.

* _digits

  A reference to `moat.kv.util.digits`.

* _info (async only)

  A queue for events. This queue receives various messages. See below.

* _log

  A standard ``Logger`` object.

* _P

  `moat.kv.util.P`, to decode a Path string to a Path object.

* _Path

  `moat.kv.util.Path`, to convert a list of path elements to a Path object.

* _self (async only)

  The controller. See `moat.kv.runner.CallAdmin`, below.

These variables, as well as the contents of the data associated with the
runner, are available as global variables.

Node Groups
===========

All runners are part of a group of nodes. The Any-Node runners use the
group to synchronize job startup.

Runners also forward the group's membership information to your code as it
changes. You can use this information to implement "emergency operation
when disconnected" or similar fallback strategies.

=========
CallAdmin
=========

Your code has access to a ``_self`` variable which contains a `CallAdmin` object.
The typical usage pattern is to start monitoring some MoaT-KV entries with
`CallAdmin.watch`, then iterate ``_info`` for the values of those entries.
When you get a `ReadyMsg` event, all values have been transmitted; you can
then set up some timeouts, set other values, access external services, and
do whatever else your code needs to do.

MoaT-KV client code requires an async context manager for most scoped
operations. Since a `CallAdmin` is scoped by definition, it can manage
these scopes for you. Thus, instead of writing boilerplate code like
this::

   import anyio
   import moat.kv.runner
   """
   Assume we want to process changes from these two subtrees
   for 100 seconds
   """
   async with _client.watch(_P("some.special.path")) as w1:
      async with _client.watch(P("some.other.path")) as w2:
         q = anyio.create_queue()  # q_s,q_r = anyio.create_memory_object_stream()
         async def _watch(w):
            async for msg in w:
               await q.put(msg)  # q_s.send(msg)
         async def _timeout(t):
            await anyio.sleep(t)
            await process_timeout()
         await _self.spawn(_watch, w1)
         await _self.spawn(_watch, w2)
         await _self.spawn(_timeout, 100)
         async for msg in q:  # q_r
            await process_data(msg)

you can simplify this to::

   await _self.watch(_P("some.special.path"))
   await _self.watch(_P("some.other.path"))
   await _self.timer(100)
   async for msg in _info:
      if msg is None:
         return  # system was stalled
      elif isinstance(msg, _cls.TimerMsg):
         await process_timeout()
      elif isinstance(msg, _cls.ChangeMsg):
         await process_data(msg.msg)

Distinguishing messages from different sources can be further simplified by
using distinct ``cls=`` parameters (subclasses of ``ChangeMsg`` and
``TimerMsg``) in your ``watch`` and ``timer`` calls, respectively.

By default, ``watch`` retrieves the current value on startup. Set
``fetch=False`` if you don't want that.

By default, ``watch`` only retrieves the named entry. Set ``max_depth=-1``
if you want all sub-entries. There's also ``min_depth`` if you should need
it.

If you use ``max_depth``, entries are returned in mostly-depth-first order.
It's "mostly" because updates may arrive at any time. A ``ReadyMsg``
message is sent when the subtree is complete.

The `CallAdmin.spawn` method starts a subtask.

`watch`, `timer`, and `spawn` each return an object which you can call
``await res.cancel()`` on, which causes the watcher, timer or task in
question to be terminated.

++++++++
Messages
++++++++

The messages in ``_info`` can be used to implement a state machine. If your
code is long-running and async, you should iterate them; if the queue is
full, your code may be halted. Alternately you'll get a `None` message.
That message indicates that the queue has stalled: you should exit.

The following message types are defined. You're free to ignore any you
don't recognize.

* CompleteState

  There are at least N runners in the group. (N is specified as an argument
  to ``run all``; making this configurable via MoaT-KV is TODO.)

* PartialState

  There are some runners available, but more than one and fewer than N.

* DetachedState

  There is no other runner available.

* BrokenState

  Something else is wrong.

* ChangeMsg

  An entry you're watching has changed. The message's ``value`` and
  ``path`` attributes contain relevant details. ``value`` doesn't exist if
  the node has been deleted.

  You can use the watcher's ``cls`` argument to subclass this message, to
  simplify dispatching.

* TimerMsg

  A timer has triggered. The message's ``msg`` attribute is the timer, i.e.
  the value you got back from ``_self.timer``. You can use `Timer.run(delay)`
  to restart the timer.

  You can use the timer's ``cls`` argument to subclass this message, to
  simplify dispatching.

* ReadyMsg

  Startup is complete. This message is generated after all watchers have
  started and sent their initial data. The ``msg`` attribute contains the
  number of watchers.

  This message may be generated multiple times because of race conditions;
  you should check that the count is correct.


The ``…State`` messages can be useful to determine what level of redundancy
you currently have in the system. One application would be to send a
warning to the operator that some nodes might be down.

