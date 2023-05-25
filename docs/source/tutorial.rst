===================
The MoaT-KV tutorial
===================

Installation
============

This part is easy. ``pip install moat-kv``.

You now have, or should have, a ``moat`` command-line utility. If not,
use this script::

   #!/usr/bin/env python3

   import sys
   # sys.path[0:0] = (".", "../asyncserf")  # for development

   import moat.__main__


You also need a running `Serf <http://serf.io>` or `MQTT
<https://mqtt.org>` message broker.  When in doubt, use MQTT.)

Start the server
================

You start an initial server with this command::

   $ moat kv server -i Testing $(hostname)
   Running.

By default, your MoaT-KV server will talk to the local MQTT process.
You can configure the destination by adapting the config file::

   $ moat kv -C kv.server.mqtt.uri=mqtt://your-server:1883 server -i Testing $(hostname)

You can now retrieve the root value::

   $ moat kv data :
   "Testing"
   $

As the purpose of MoaT-KV is to be a *distributed* key-value storage, 
you can start another server on a different host::

   two $ moat kv -C server.mqtt.uri=mqtt://your-server:1883 server $(hostname)
   Running.


This will take a few seconds for the servers to sync up with each other.
You can verify that the second server has successfully synced up::

   two $ moat kv data :
   "Testing"
   two $

The root value is not special; by convention, it contains some data about the current
MoaT-KV network.

You can now kill the first server and restart it::

   $ moat kv server $(hostname)
   Running.

You must **never** start a server with the ``-i`` option unless you're
creating a new and separate MoaT-KV network.

You can create separate networks by changing the ``server.root`` config
variable. Such networks do not collide with each other, other than sharing
Serf gossip bandwidth.


Data commands
=============

You might want to add an alias for "moat kv data" so that you don't
have to type so much. In ``bash``::

   $ mkd() { moat kv data "$@"; }

Then, you can store arbitrary data at random MoaT-KV nodes::

   $ mkd one.two.three set -e : 123
   $ mkd one.two.three.four set -e : 1234
   $ mkd one.two.three.four.five set -v one XXX -e two 2
   $ mkd one.two.three
   123
   $ mkd one.two.three.four.five
   one: XXX
   two: 2
   $

The ``-e`` flag tells the ``set`` command to evaluate the given data as a
Python expression. You can store numbers, True/False/None, binary and
Unicode strings, and lists/tuples/hashes composed of these.

Stored values may be data structures, and you can selectively change them::

   $ mkd one.two.three.four.five set -e one 1
   $ mkd one.two.three.four.five
   one: 1
   two: 2

The colon we used after ``-e`` is the empty path. More about paths below.

All entries' values are independent. MoaT-KV's storage is organized
hierarchically, (among other reasons) for ease of retrieval::

    $ mkd one get -rd_
    two:
      three:
        _: 123
        four:
          _: 1234
          five:
            _: 
              one: 1
              two: 2
    $

MoaT-KV also stores some internal data, under a special ``null`` root key.
You can use ``moat kv internal dump :`` to display them.

Path specification
------------------

MoaT-KV uses "paths" to access entries (and the partial values in them).
We chose the dot as a path separator because it's more visually distinctive
than a slash.

In MoaT-KV, paths elements are not limited to strings; integers can
also be path elements, as can ``True``, ``False``, ``None``, and tuples
composed from them. We use colons instead of dots to mark those.
The colon is also used as an escape characters for path elements that
contain dots or colons; it is easy to type and doesn't occur often,
while the traditional Unix escape character (backslash ``\\``) is
hard to type in some locales and must be duplicated almost everywhere you
want to actually use it.

A space is encoded as ``:_``. While a literal space is not a problem, it
needs to be escaped on the command line. Experience shows that people tend
to forget that. A "real" underscore ``_`` is not escaped.

There's also the empty path (i.e. the top of MoaT-KV's entry hierarchy,
not the same as a path that consists of an empty-string element!) which is
coded as a stand-alone ``:`` for much the same reason.

Anything else that follows a colon is evaluated as a Python expression.

Thus:

==== ==========
Code   Meaning
---- ----------
 :.  literal ``.``
 ::  literal ``:``
 :_    space
==== ==========
 :t    True
 :f    False
 :n    None
 :e  empty string
 :x  hex integer
 :b  binary integer
 :y  hex bytestring
 :v  literal bytestring
 :XX eval(XX)

==== ==========

The first three are inline escape sequences while the others start a new
element.

Hex number input is purely a convenience; integers in paths are always
printed in decimal form. While you also could use ``:0x…`` in place of
``:x…``, the latter reduces visual clutter:

.. warning::

   Yes, MoaT-KV supports tuples as part of paths. You probably should not use
   this feature without a very good reason. "My key consists of three
   random integers and I want to avoid the overhead of storing a lot of
   intermediate entries" would be an example of a good reason.
   
   MoaT-KV also allows you to use both ``False``, an integer zero, and a
   floating-point zero as path elements. This is dangerous because Python's
   comparison and hashing operators treat them as being equal. (Same for
   ``True`` and 1; same for floating point numbers without fractions and
   the corresponding integers.)

   Floating point numbers are also dangerous for a different reason: floats 
   that are not a fractional power of two, such as 1/3, are inexact.
   Thus you might end up with five different entries for what was meant to
   be ``1/3``.

   Bottom line:

   * If you do need paths elements with sub-integer numbers, consider
     scaling them up using using ``int(num*1000)``, or fractional numbers
     (stored as a numerator,denominator tuple), or ``str(Decimal(…))``.

   * Don't use multiple numeric types as child nodes of a single parent.


Persistent storage
==================

MoaT-KV keeps everything in memory.

As this is not optimal if there is a power failure (or, for single-node
systems, a server crash or OS update or …), MoaT-KV has a built-in
mechanism to save its state to disk.

Automatic state save+restore
----------------------------

Do this::

    $ echo MODE=hybrid >>/etc/moat/kv.env
    $ /usr/lib/moat/kv/rotate
    $ systemctl restart moat-kv

The MoaT-KV server will now auto-save the current state every 15 minutes,
log all changes, and load the most-recent state from disk when it's
restarted.

Use ``MODE=master`` if you use a stand-alone MoaT server.

Manual state save+restore
-------------------------

You can also save state manually::

   $ moat kv log dest /var/local/lib/moat/kv/$(date +%Y%m%d).state

This command writes the current state to the given file. The server keeps the
file open and appends new records to it. The ``log dest`` has options to
write an incremental change record or to create a one-shot dump.

Incremental records are guaranteed to not have missing or duplicate records.

When you need to restart your MoaT-KV system from scratch, tell it to use the
newest saved state file::

    $ moat kv server -l $(ls -t /var/local/lib/moat/kv/*.state | head -1) $(hostname)
    Running.

If your state dump files are incremental, you should instead do
something like this::

    $ moat kv server -l <(cat /var/local/lib/moat/kv/*.state) $(hostname)
    Running.

These commands are mostly-safe to use on a network that's already
running; your node may run with old state for a few seconds until it
retrieves the updates that happened while it was down. An option to delay
startup until that process has completed is somewhere on the TODO list.

In a typical MoaT-KV network, at most two or three nodes will use persistent
storage; all others simply sync up with one of their peers whenever they
are restarted.


Authorization
=============

MoaT-KV initially doesn't use an authorization scheme. However,
advanced uses require the ability to distinguish between users.

Let's set up a "root" user::

    $ moat kv auth -m password user add name=joe password?=Code
    Code: ******
    $ moat kv auth -m password user list
    joe
    $ moat kv auth -m password init -s
    Authorization switched to password
    $

(The input at the "Code:" prompt is not echoed.)

After this point, you can no longer use MoaT-KV without a password::

    $ mkd :
    ClientAuthRequiredError: You need to log in using: password
    $

    $ moat kv -a "password name=joe password?=Code" data :
    Code: ******
    "Root"
    $

Internal data are stored in a separate MoaT-KV subtree that starts with a ``None`` value.
You can display it::

    $ moat kv -a "password name=joe password=test123" data internal dump :
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

   $ moat kv -a "password name=joe password=test123" data null.foo set -v : bar
   $ moat kv -a "password name=joe password=test123" data : get -rd_
   …
   'null':
     foo:
       _: bar

For experimentation, there's also a ``_test`` authorization method which
only exposes a user name::

   $ moat kv auth -m _test user add name=joe
   $ moat kv auth -m _test user add name=root
   $ moat kv auth -m _test init
   $ moat kv data :
   ClientAuthRequiredError: You need to log in using: _test
   $ mkv() { moat kv -a "_test name=joe" "$@"; }
   $ mkv data :
   123
   $

We'll use this user, and the shell alias, in the following sections.

ACLs and distributed servers
----------------------------

MoaT-KV servers actually use the client protocol when they sync up. Thus, when you
set up authorization, you must teach your servers to authenticate to their
peer::

   $ moat kv -C connect.auth="_test name=joe" server $(hostname)

You typically store that in a configuration file::

    kv:
      conn:
        auth: "_test name=joe"
        host: 127.0.0.1

``moat`` auto-reads the configuration from a few paths, or you can use
the ``moat -c test.cfg`` flag.

Access restrictions
===================

A user can be restricted from accessing or modifying MoaT-KV data.

Let's say that we'd like to create a "write-only" data storage::

   $ moat kv -a "_test name=root" acl set writeonly -a xc 'wom.#'
   $ moat kv -a "_test name=root" auth user set param joe acl writeonly
   $ mkv data wom.foo.bar set -e : 42
   $ mkv data wom.foo.bar set -e : 43
   ServerError: (<AclEntry:[None, 'acl', 'writeonly', 'wom', '#']@<NodeEvent:<Node: test1 @10> @4 1> ='cx'>, 'w')
   $ mkv data wom.foo
   ServerError: (<AclEntry:[None, 'acl', 'writeonly', 'wom', '#']@<NodeEvent:<Node: test1 @10> @4 1> ='cx'>, 'r')
   $

As you can see, this allows the user to write to arbitrary values to the
"wom" tree, but Joe cannot change anything – nor can he read the values
which he wrote.

Note that we also created a "root" user who doesn't have ACL restrictions.
If we had not, we'd now be locked out of our MoaT-KV storage because "no
matching ACL" means "no access".

A user who has an ACL set can no longer modify the system, because the
``None`` element that separates system data from the rest cannot match a
wildcard. ACLs for system entries are on the TODO list; so are user groups
or roles or whatever. Code welcome.



Code execution
==============

MoaT-KV doesn't just store passive data: you can also use it to distribute
actual computing. We'll demonstrate that here.

First we feed some interesting code into MoaT-KV::

    $ mkv code set the.answer <<END
    > print("Forty-Two!")
    > return 42
    > END

Then we set up a one-shot run-anywhere instance::

   $ mkv run set -c the.answer -t 0 a.question

This doesn't actually execute any code because the executor is not part of
the MoaT-KV server. (The server may gain an option to do that too, but
not yet.) So we run it::

   $ mkv run all
   Forty-Two!

(Initially this takes some time, because the ``run`` command needs to
co-ordinate with other runners. There aren't any, others, of course, but
MoaT-KV can't know that.)

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
code on one specific node; simply do::

   $ mkv run -n $(hostname) set -c "same answer" -t 0 a.question
   $ mkv run -n $(hostname) all

The one-node-only runner and the any-node runner are distinct. There's also
a way to designate a subgroup of hosts (like "all with a 1wire interface")
and to run a job on any / all of them. See ``moat kv run --help`` for details.


Errors
======

Nobody is perfect, and neither is code. Sometimes things break.
MoaT-KV remembers errors. To demonstrate, let's first provoke one::

    $ mkv code set the.error <<END
    > raise RuntimeError("Owch")
    > END
    $ mkv run set -c the.error -t 0 what.me.worry
    $ mkv run all  # if it's not still running
    20:24:13.935 WARNING:moat.kv.errors:Error ('.moat', 'kv', 'error', 'test1', 16373) test1: Exception: Owch

The list of errors is now no longer empty::

   $ mkv error list -d_
   [ some YAML ]

You can limit the error list to specific subtrees. This command has the
same effect::

   $ mkv error list -d_ :.moat.kv.run.any

except that the path is shortened for improved useability.

Error details are available; add the ``-a`` option. You can also filter
errors on a specific node, which only includes that node's details.


The Python API
==============

Command lines are all well and good, but MoaT-KV gets really interesting
when you use it from Python.

Let's start by simply setting some value::

   import anyio
   from moat.kv.client import open_client
   from moat.util import P

   async def dkv_example():
      async with open_client() as client:
         client.set(P("one.two.three"), value=("Test",42,False), chain=None)

   anyio.run(dkv_example)

That was easy. Now we'd like to update that entry::

   from moat.util import P
   async def dkv_example():
      async with open_client() as client:
         res = client.get(P("one.two.three"), nchain=2)
         ret = client.set(P("one.two.three"), value=("Test",v[1]+1,False), chain=res.chain)
         assert res.chain != ret.chain

The ``chain`` parameter is important: it tells MoaT-KV which change caused
the old value. So if somebody else changes your ``one.two.three`` entry
while your program was running, you get a collision and the ``set`` fails.

``set`` returns a new chain so you can update your value multiple times.

Deleting an entry clears the chain because the source of a non-existing value
doesn't matter.

.. warning::
   MoaT-KV is an asynchronous distributed system. Thus, asuming that you
   have more than one MoaT-KV server, this does not prevent your ``set``
   command from being ignored; it just reduces the window when this could
   happen from the time since the last ``get`` to a couple of milliseconds.


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
to be tedious. MoaT-KV comes with a couple of classes that does this for you::

   from moat.kv.obj import ClientRoot, ClientEntry
   from moat.util import NotGiven

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

            await anyio.sleep_forever()
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

    $ ./kv client type set -g 0 -g -2 -g 123 -b 1.2 -b '"Hello"' int <<END
    > if int(value) != value: raise ValueError("not an integer")
    > END
    $

As you can see, data types must be accompanied by example values that include
both "good" and "bad" examples.

You can also declare subtypes::

    $ mkv type set -g 0 -g 99 -g 100 -b -1 -b 101 int.percent <<END
    > if not (0 <= value <= 100): raise ValueError("not a percentage")
    > END
    $

The example values, both good and bad, must pass the supertype's checks.

Now we associate the test with our data::

    $ mkv type match -t int.percent 'stats.#.quota'

Then we store some value::

    $ mkv data stats.foo.bar.quota set -v : 123
    ServerError: ValueError("not an integer")

Oops: non-string values need to be evaluated. Better::

    $ mkv data stats.foo.bar.quota set -e : 123
    ServerError: ValueError('not a percentage')
    $ mkv data stats.foo.bar.quota set -e : 12
    $

MoaT-KV does not test that existing values match your restrictions.


Data mangling
=============

Structured data are great, but some clients want boring single-value items.
For instance, some home automation systems want to use ``"ON"`` and
``"OFF"`` messages, while your active code is much happier with a ``bool``
value – or even a mapping that also carries the time of last change, so that
a ``turn off after 15 minutes`` rule will actually work.

Let's write a simple number codec::

    $ mkv codec set -i '"12.5"' 12.5 -o 13.25 '"13.25"' float.str
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

    $ mkv codec convert -c float.str floatval 'monitor.#.value'

This associates

* the float-to-string codec we just created

* all paths that start with ``monitor`` and end with ``value``

with the codec list named ``floatval``. As not every user needs stringified
numbers, we also need to tell MoaT-KV which users to apply this codec to::

    $ mkv auth user modify --aux codec=floatval name=joe
	
Thus, Joe will read and write ``value`` entries as strings::

    $ mkv data monitor.a.b.c.value set -v : 99.5
    $ mkv data monitor.a.b.c.thing set -v : 12.3
    $ mkv data monitor get -rd_
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
receives and transmits strings. A real-world application would use
binary strings, not Unicode strings.


Limitations
-----------

MoaT-KV currently can't translate paths, or merge many values to one entry's attributes.

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

For some use cases, you might want to configure MoaT-KV dynamically instead
of by a static configuration file.

This is not always feasible; in particular, the "logging" and "server"
sections are imported once. Also, options used for connecting to another
MoaT-KV server cannot be set dynamically because you need them before the
data are available.

Other options may be overridden by storing a new values at ``.moat kv config
<name>``. It is not possible to be more specific. (TODO)

If a client's ACLs do not allow reading a config entry, it will be silently
ignored.

A config entry's ``_watch`` property will trigger when the entry is updated.
