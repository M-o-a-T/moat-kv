===================
The DistKV tutorial
===================

Installation
============

This part is easy. ``pip install distkv``.

You now have, or should have, a ``distkv`` command-line utility. If not,
use this script::

   #!/usr/bin/env python3

   __requires__ = 'distkv'
   import re
   import sys
   import os
   sys.path[0:0] = (".", "../asyncserf")

   try:
      from pkg_resources import load_entry_point
   except Exception as exc:
      pass
   else:
      sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
      sys.exit(load_entry_point('distkv', 'console_scripts', 'distkv')())

   from distkv.command import cmd
   cmd()


You also need a running `Serf <http://serf.io>` daemon.

Start the server
================

You start an initial server with this command::

   one $ distkv server -i Root $(hostname)
   Running.

By default, your DistKV server will talk to the local Serf process.
You can configure the destination by adapting the config file::

   one $ distkv -C server.serf.host=my-serfer server -i Root $(hostname)

You can then retrieve the root value::

   one $ distkv client data get
   "Root"
   one $

As the purpose of DistKV is to be a *distributed* key-value storage, 
you can start another server on a different host::

   two $ distkv server $(hostname)
   Running.


This will take a few seconds for the servers to sync up with each other.
You can verify that the second server has successfully synced up::

   two $ distkv client data get
   "Root"
   two $

You can now kill the first server and restart it::

   one $ killall distkv
   one $ distkv server $(hostname)
   Running.

You must **never** start a server with the ``-i`` option, unless you're
creating a new and separate DistKV network. (You can create entirely
separate networks with the ``server.root`` config variable.)


Data commands
=============

You might want to add an alias for "distkv client data" so that you don't
have to type so much::

   one $ cat >/usr/local/bin/dkd <<'_'
   #!/bin/sh
   exec distkv client data "$*"
   _

   one $ chmod +x /usr/local/bin/dkd

Then, you can store arbitrary data at random DistKV nodes::

   one $ dkd set -ev 123 one two three
   one $ dkd set -ev 1234 one two three four
   one $ dkd set -v Duh one two three four five
   one $ dkd get one two three
   123
   one $ dkd get one two three four five
   "Duh"
   one $

The ``-e`` flag tells the ``set`` command to evaluate the given data as a
Python expression. You can store numbers, True/False/None, binary and
Unicode strings, lists/tuples, and hashes composed of these.

All values are independent. DistKV's storage is still organized
hierarchically, (among other reasons) for ease of retrieval::

    one $ dkd get -ryd_ one
    one:
      two:
        three:
          _: 123
          four:
            _: 1234
            five:
              _: Duh
    one $


Persistent storage
==================

DistKV keeps everything in memory (for now). If you want your data to
survive a power outage, you might want to tell your server to save them::

   one $ distkv client log dest /var/local/lib/distkv.$(date +%Y%m%d).state

This command writes the current state to this file. The server keeps it
open and appends new records to it. The ``log dest`` has options to either
not start with a complete state dump, or to just write a one-shot dump.

When you need to restart your DistKV system from scratch, simply pass the
newest saved state file::

    one $ distkv server -l $(ls -t /var/local/lib/distkv.*.state | head -1) $(hostname)
    Running.

This command is somewhat safe to use on a network that's already running;
your node may run with old state for a few seconds, until it retrieves the
updates that happened while it was down. An option to delay startup until
that process has completed is on the TODO list.

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

    one $ ./kv client -a "password name=joe password?=Code" data get
    Code: ******
    "Root"
    one $

Internal data are stored in a separate DistKV subtree that starts with a ``None`` value.
You can display it::

    one $ distkv client -a "password name=joe password=test123" data get -ryd_
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
    one:
      two:
        three:
          _: 123
          four:
            _: 1234
            five:
              _: Duh
    
As you can see, passwords are encrypted -- hashed, actually. The exact
scheme depends on the auth method. The data below ``None`` (or "null" in
YAML syntax) are otherwise inaccessible.

NB: nothing prevents you from using the string ``"null"`` as an ordinary
key name::

   one $ distkv client -a "password name=joe password=test123" data set -v bar null foo
   one $ distkv client -a "password name=joe password=test123" data get -ryd_
   …
   'null':
     foo:
       _: bar


Access restrictions
===================

TODO

A user can be restricted to either access DistKV data, or to modify
the server itself. 