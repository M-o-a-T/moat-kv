===================
The DistKV tutorial
===================

Installation
============

This part is easy. ``pip install distkv``.

You now have, or should have, a ``distkv`` command-line utility. If not,
use this script::

   #!/usr/bin/env python3

   import sys
   # sys.path[0:0] = (".", "../asyncserf")  # for development

   from distkv.command import cmd
   cmd()


You also need a running `Serf <http://serf.io>` daemon.

Start the server
================

You start an initial server with this command::

   one $ distkv server -i Testing $(hostname)
   Running.

By default, your DistKV server will talk to the local Serf process.
You can configure the destination by adapting the config file::

   one $ distkv -C server.serf.host=my-serfer server -i Testing $(hostname)

You can now retrieve the root value::

   one $ distkv client data get
   "Testing"
   one $

As the purpose of DistKV is to be a *distributed* key-value storage, 
you can start another server on a different host::

   two $ distkv server $(hostname)
   Running.


This will take a few seconds for the servers to sync up with each other.
You can verify that the second server has successfully synced up::

   two $ distkv client data get
   "Testing"
   two $

(NB: The root value is not special; by convention, it identifies the DistKV
network.)

You can now kill the first server and restart it::

   one $ killall distkv
   one $ distkv server $(hostname)
   Running.

You must **never** start a server with the ``-i`` option unless you're
creating a new and separate DistKV network.

You can create separate networks by changing the ``server.root`` config
variable. Such networks do not collide with each other, other than sharing
Serf gossip bandwidth.


Data commands
=============

You might want to add an alias for "distkv client data" so that you don't
have to type so much. In ``bash``::

   one $ dkd() { distkv client data "$@"; }

Then, you can store arbitrary data at random DistKV nodes::

   one $ dkd set -ev 123 one.two.three
   one $ dkd set -ev 1234 one.two.three.four
   one $ dkd set -v Duh one.two.three.four.five
   one $ dkd get one.two.three
   123
   one $ dkd get one.two.three.four.five
   "Duh"
   one $

The ``-e`` flag tells the ``set`` command to evaluate the given data as a
Python expression. You can store numbers, True/False/None, binary and
Unicode strings, lists/tuples, and hashes composed of these.

All entries' values are independent. DistKV's storage is organized
hierarchically, (among other reasons) for ease of retrieval::

    one $ dkd get -rd_ one
    two:
      three:
        _: 123
        four:
          _: 1234
          five:
            _: Duh
    one $

DistKV's internal data are stored under a special ``null`` root key.
You can use ``distkv client internal dump :`` to display them. This command
behaves like ``distkv client data get -rd_``. It too accepts a path prefix.

Path specification
------------------

You might wonder what to do when a path element contains a dot. Our
solution is to prefix it with an escape character: a colon (``:``).
Thus, a path consisting of 'a', 'b.c' and 'd' is written as ``a.b:.c.d``.
We choose a colon because it is easy to type and doesn't occur often.

The traditional Unix escape character (backslash ``\\``) is not easy to
type and must be duplicated almost everywhere you want to actually type it,
thus we don't use that. You may need it to shell-escape spaces or quotes in
paths, however.

Of course, you now need to escape colons too: the path 'a' 'b:c' 'd' is
written as ``a.b::c.d``.

Colons have other uses because ``True``, ``False``, ``None``, arbitrary
numbers, or even lists can also be path elements. Also, DistKV codes the empty
string as ``:e`` – otherwise it'd be too easy to leave a stray or duplicate
dot at the end of a path and then wonder why your data are missing.

A space is encoded as ``:_``. While a literal space is not a problem, it
needs to be escaped on the command line. Experience shows that people tend
to skip that.

There's also the empty path (i.e. the top of DistKV's entry hierarchy,
not the same as a path that consists of an empty string!), which is
coded as a single colon for much the same reason.

Thus:

==== ==========
Code   Meaning
---- ----------
 :.      .
 ::      :
 :_    space
 :t    True
 :f    False
 :n    None
 :e    empty
 :x  hex number
==== ==========

If anything else follows your colon, it's evaluated as a Python expression
and added to the path.

Hex number input is purely a convenience; integers in paths are always
printed in decimal form. While you also could use ``:0x…`` in place of
``:x…``, the latter reduces visual clutter and ensures that the input is in
fact a hex number and not something else by mistake.

.. warning::

   Yes, DistKV supports tuples as part of paths. You probably should not use
   this feature without a very good reason. "My key consists of three
   random integers and I want to avoid the overhead of storing a lot of
   intermediate entries" would be an example of a good reason.
   
   DistKV also allows you to use both ``False``, an integer zero, and a
   floating-point zero as path elements. This is dangerous because Python's
   comparison and hashing operators treat them as equal. (Same for ``True``
   and 1; same for floating point numbers without fractions and the
   corresponding integers.)

   Floating point numbers are also dangerous for a different reason: floats 
   that are not a fractional power of two, like 1/3, cannot be stored
   exactly. Thus you might have problems entering them.

   Bottom line:

   * Don't use inexact fractions. 1/2 and 1/4 is fine, 1/3 or 1/5 is not.

   * Don't use multiple types as keys on the same level.


Persistent storage
==================

DistKV keeps everything in memory (for now). If you want your data to
survive a power outage, you might want to tell your server to save them::

   one $ distkv client log dest /var/local/lib/distkv.$(date +%Y%m%d).state

This command writes the current state to this file. The server keeps the
file open and appends new records to it. The ``log dest`` has options to
either write an incremental change record, or to just write a one-shot
dump.

When you need to restart your DistKV system from scratch, simply pass the
newest saved state file::

    one $ distkv server -l $(ls -t /var/local/lib/distkv.*.state | head -1) $(hostname)
    Running.

Obviously, if your state dump files are incremental, you should instead do
something like this::

    one $ distkv server -l <(cat /var/local/lib/distkv.*.state) $(hostname)
    Running.

These commands are somewhat safe to use on a network that's already
running; your node may run with old state for a few seconds until it
retrieves the updates that happened while it was down. An option to delay
startup until that process has completed is on the TODO list.

In a typical DistKV network, at most two or three nodes will use persistent
storage; all others simply syncs up with their peers whenever they are
restarted.


Authorization
=============

DistKV initially doesn't come up with any authorization scheme. However,
advanced uses require the ability to distinguish between users.

Let's set up a "root" user::

    one $ distkv client auth -m password user add name=joe password?=Code
    Code: ******
    one $ distkv client auth -m password user list
    joe
    one $ distkv client auth -m password init -s
    Authorization switched to password
    one $

(The input at the "Code:" prompt is not echoed.)

After this point, you can no longer use DistKV without a password::

    one $ dkd get
    ClientAuthRequiredError: You need to log in using: password
    one $

    one $ distkv client -a "password name=joe password?=Code" data get
    Code: ******
    "Root"
    one $

Internal data are stored in a separate DistKV subtree that starts with a ``None`` value.
You can display it::

    one $ distkv client -a "password name=joe password=test123" data internal dump :
    null:
      auth:
        _:
          current: password
        password:
          user:
            joe:
              _:
                _aux: null
                password: !!binary |
                  7NcYcNGWMxapfjrDQIyYNa2M8PPBvHA1J8MCZVNPda4=

As you can see, passwords are encrypted -- hashed, actually. The exact
scheme depends on the auth method.

NB: nothing prevents you from using the string ``"null"`` as an ordinary
key name::

   one $ distkv client -a "password name=joe password=test123" data set -v bar null.foo
   one $ distkv client -a "password name=joe password=test123" data get -rd_ :
   …
   'null':
     foo:
       _: bar

For experimentation, there's also a ``_test`` authorization method which
only exposes a user name::

   one $ distkv client auth -m _test user add name=joe
   one $ distkv client auth -m _test user add name=root
   one $ distkv client auth -m _test init
   one $ distkv client data get
   ClientAuthRequiredError: You need to log in using: _test
   one $ dkv() { distkv client -a "_test name=joe" "$@"; }
   one $ dkv data get :
   123
   one $

We'll use that user and alias in the following sections.

ACLs and distributed servers
----------------------------

DistKV servers actually use the client protocol when they sync up. Thus, when you
set up authorization, you must teach your servers to authenticate to their
peer::

   one $ distkv -C connect.auth="_test name=joe" server $(hostname)

You typically store that in a configuration file::

    connect:
        auth: "_test name=joe"
        host: 127.0.0.1

``distkv`` auto-reads the configuration from a few paths, or you can use
the ``-c test.cfg`` flag.

Access restrictions
===================

A user can be restricted from accessing or modifying DistKV data.

Let's say that we'd like to create a "write-only" data storage::

   one $ distkv client -a "_test name=root" acl set writeonly -a xc 'wom.#'
   one $ distkv client -a "_test name=root" auth user set param joe acl writeonly
   one $ dkv data set -ev 42 wom.foo.bar
   one $ dkv data set -ev 43 wom.foo.bar
   ServerError: (<AclEntry:[None, 'acl', 'writeonly', 'wom', '#']@<NodeEvent:<Node: test1 @10> @4 1> ='cx'>, 'w')
   one $ dkv data get wom.foo
   ServerError: (<AclEntry:[None, 'acl', 'writeonly', 'wom', '#']@<NodeEvent:<Node: test1 @10> @4 1> ='cx'>, 'r')
   one $

As you can see, this allows the user to write to arbitrary values to the
"wom" tree, but Joe cannot change anything – nor can he read the values
which he wrote.

Note that we also created a "root" user who doesn't have ACL restrictions.
If we had not, we'd now be locked out of our DistKV storage because "no
matching ACL" means "no access".

A user who has an ACL set can no longer modify the system, because the
``None`` element that separates system data from the rest cannot match a
wildcard. ACLs for system entries are on the TODO list; so are user groups
or roles or whatever. Code welcome.



Code execution
==============

DistKV doesn't just store passive data: you can also use it to distribute
actual computing. We'll demonstrate that here.

First we feed some interesting code into DistKV::

    one $ dkv code set the.answer <<END
    > print("Forty-Two!")
    > return 42
    > END

Then we set up a one-shot run-anywhere instance::

   one $ dkv run set -c the.answer -t 0 a.question

This doesn't actually execute any code because the executor is not part of
the DistKV server. (The server may gain an option to do that too, but
not yet.) So we run it::

   one $ dkv run all
   Forty-Two!

(Initially this takes some time, because the ``run`` command needs to
co-ordinate with other runners. There aren't any, others, of course, but
DistKV can't know that.)

The code will not run again unless we either re-set ``--time``, or set a
repeat timer with ``--repeat``.

Start times are mostly-accurate. There are two reasons why they might not
be:

* the co-ordination system has a periodic window where it waits for the
  next coordinator. This causes a delay of up to two seconds.

* TODO: The current leader might decide that it's too busy and wants to
  delegate starting a particular job to some other node in the cluster.
  This incurs some delay, more if the recipient is no longer available.

This method will run the code in question on any node. You can also run
code on one specific node; simply do

   one $ dkv run -n $(hostname) set -c "same answer" -t 0 a.question
   one $ dkv run -n $(hostname) all

The one-node-only runner and the any-node runner are distinct. There's also
a way to designate a subgroup of hosts (like "all with a 1wire interface")
and to run a job on any / all of them. See ``dkv run --help`` for details.


Errors
======

Nobody is perfect, and neither is code. Sometimes things break.
DistKV remembers errors. To demonstrate, let's first provoke one::

    one $ dkv code set the.error <<END
    > raise RuntimeError("Owch")
    > END
    one $ dkv run set -c the.error -t 0 what.me.worry
    one $ dkv run all  # if it's not still running
    20:24:13.935 WARNING:distkv.errors:Error ('.distkv', 'error', 'test1', 16373) test1: Exception: Owch

The list of errors is now no longer empty::

   one $ dkv error list -d_
   [ some YAML ]

You can limit the error list to specific subtrees. This command has the
same effect::

   one $ dkv error list -d_ :.distkv.run.any

except that the path is shortened for improved useability.

Error details are available; add the ``-a`` option. You can also filter
errors on a specific node, which only includes that node's details.


The Python API
==============

Command lines are all well and good, but DistKV gets really interesting
when you use it from Python.

Let's start by simply setting some value::

   import anyio
   from distkv.client import open_client

   async def dkv_example():
      async with open_client() as client:
         client.set(("one","two","three"), value=("Test",42,False), chain=None)

   anyio.run(dkv_example)

That was easy. Now we'd like to update that entry::

   from distkv.util import P
   async def dkv_example():
      async with open_client() as client:
         res = client.get(P("one.two.three"))
         ret = client.set(P("one.two.three"), value=("Test",v[1]+1,False), chain=res.chain)
         assert res.chain != ret.chain

The ``chain`` parameter is important: it tells DistKV which change caused
the old value. So if somebody else changed your ``one.two.three`` entry
while your program was running, you'd get a collision and the ``set`` would
fail.

``set`` returns a new chain so you can update your value multiple times.

Deleting an entry clears the chain because the source of a non-existing value
doesn't matter.

Watching for Changes
--------------------

The result of the previous ``get`` was static. If somebody else
subsequently changes it, you wouldn't know. Let's fix that::

   async def dkv_example():
      async with open_client() as client:
         async with client.watch(P("one.two"), fetch=True) as watcher:
            async for res in watcher:
               if 'path' not in res:
                  continue
               if 'value' in res:
                  print(f"{path}= {res.value}")
               else:
                  print(f"{path}: deleted")

``fetch=True`` will send the current state in addition to any changes.
The ``'path' not in res`` test filters the notification that tells you that
the subtree you requested is complete. The result's path doesn't contain
the prefix you used in ``watch`` because you already know it.

if you need two ``watch`` at the same time, create separate tasks. Feed the
resuts through a common queue if you want to process them in a comon
function.

Active objects
--------------

While watching for changes is nice, organizing the resulting objects tends
to be tedious. DistKV comes with a couple of classes that does this for you::

   from distkv.obj import ClientRoot, ClientEntry
   from distkv.util import NotGiven

   class OneEntry(ClientEntry):
      async def set(self, value):
         await super().set_value()
         path = ' '.join(str(x) for x in self.subpath)
         if value is NotGiven:
            print(f"{path}= {value}")
         else:
            print(f"{path}: deleted")

   class OneRoot(ClientRoot):
      @classmethod
      def child_type(cls, name):
         return OneEntry

   async def dkv_example():
      async with open_client() as client:
         async with client.mirror("one", root_type=OneRoot) as root:
            # At this point you have the sub-tree in memory
            assert root['two']['three'].value[1] >= 42

            while True:
               await anyio.sleep(99999)
         pass
         # at this point the sub-tree is still there, but won't be updated

except that in a real program you'd do some real work instead of sleeping.

Verification
============

Complex data should be clean. Storing ``"Hello there!"`` in a value that
the rest of your code expects to be an integer is likely to have unwanted
effects.

For this example, we'd like to enforce that all ``quota`` values in our
site statistics are integer percentages.

First, we define the type::

    one $ ./kv client type set -g 0 -g -2 -g 123 -b 1.2 -b '"Hello"' int <<END
    > if int(value) != value: raise ValueError("not an integer")
    > END
    one $

As you can see, data types must be accompanied by example values that include
both "good" and "bad" examples.

You can also declare subtypes::

    one $ dkv type set -g 0 -g 99 -g 100 -b -1 -b 101 int.percent <<END
    > if not (0 <= value <= 100): raise ValueError("not a percentage")
    > END
    one $

The example values, both good and bad, must pass the supertype's checks.

Now we associate the test with our data::

    one $ dkv type match -t int.percent 'stats.#.quota'

Then we store some value::

    one $ dkv data set -v 123 stats.foo.bar.quota
    ServerError: ValueError("not an integer")

Oops: non-string values need to be evaluated. Better::

    one $ dkv data set -ev 123 stats.foo.bar.quota
    ServerError: ValueError('not a percentage')
    one $ dkv data set -ev 12 stats.foo.bar.quota
    one $

DistKV does not test that existing values match your restrictions.


Data mangling
=============

Structured data are great, but some clients want boring single-value items.
For instance, some home automation systems want to use ``"ON"`` and
``"OFF"`` messages, while your active code is much happier with a ``bool``
value – or even a mapping that also carries the time of last change, so that
a ``turn off after 15 minutes`` rule will actually work.

Let's write a simple number codec::

    one $ dkv codec set -i '"12.5"' 12.5 -o 13.25 '"13.25"' float.str
    Enter the Python script to encode 'value'.
    return str(value)
    Enter the Python script to decode 'value'.
    return float(value)
    ^D

As you can see, you need to give the codec some examples. Here they're
symmetric but that's not a requirement; for instance, a ``bool`` codec for our
home automation system could accept a wide range of ``true``-ish or
``false``-ish strings but it would always output ``ON`` and ``OFF``.

Associating this codec with a path is slightly more involved::

    one $ dkv codec convert -c float.str floatval 'monitor.#.value'

This associates

* the float-to-string codec we just created

* all paths that start with ``monitor`` and end with ``value``

with the codec list named ``floatval``. As not every user needs stringified
numbers, we also need to tell DistKV which users to apply this codec to::

    one $ dkv auth user modify --aux codec=floatval name=joe
	
Thus, Joe will read and write ``value`` entries as strings::

    one $ dkv data set -v 99.5 monitor a b c value
    one $ dkv data set -v 12.3 monitor a b c thing
    one $ dkv data get -rd_ monitor
    a:
      b:
        c:
          value:
            _:
              99.5
          thing:
            _:
              '12.3'

This is especially helpful if Joe is in fact an MQTT gateway which only
receives and transmits strings, though a real-world application would use
binary strings, not Unicode strings.


Limitations
-----------

DistKV currently can't translate paths, or merge many values to one entry's attributes.

You can use either active objects (add some code to their ``set_value``
methods) or code objects (listen to A and write to B) to effect such
translations. There are some caveats:

* All such data are stored twice.

* Replacing a value with the exact same value still counts as a change.
  Don't set up an endless loop.

* You need to verify that the two trees match when you start up, and decide
  which is more correct. (The ``tock`` stamp will help you here.) Don't
  overwrite changes that arrive while you do that.


Dynamic configuration
=====================

For some use cases, you might want to configure DistKV dynamically instead
of by a static configuration file.

This is not always feasible; in particular, the "logging" and "server"
sections are imported once. Also, options used for connecting to another
DistKV server cannot be set dynamically because you need them before the
data are available.

Other options may be overridden by storing a new values at ``.distkv config
<name>``. It is not possible to be more specific. (TODO)

If a client's ACLs do not allow reading a config entry, it will be silently
ignored.

A config entry's ``_watch`` property will trigger when the entry is updated.
