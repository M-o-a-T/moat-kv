==================
The DistKV command
==================

DistKV exports one command line tool, appropriately named ``distkv``. It
provides various sub- and sub-sub-commands to start and control your server.

Each command and sub-command accepts a distinct set of options which must
be used directly after the (sub)command affected by them.

distkv
======

.. program:: distkv

The main entry point for all commands.

Subcommands:

  * :program:`distkv client`

  * :program:`distkv server`

.. option:: -v, --verbose

   Increase debugging verbosity. Broadly speaking, the default is
   ``ERROR``; this option selects ``WARNING``, ``INFO`` or ``DEBUG``
   depending on how often you use it.

.. option:: -q, --quiet

   Decrease debugging verbosity: the opposite of :option:`distkv -v`,
   reducing the verbosity to ``FATAL``.

.. option:: -l, --log <source=LEVEL>

   You can selectively adjust debugging verbosity if you need to print, or
   ignore, some types of messages. Example: ``-vvv --log
   asyncserf.actor=ERROR`` would suppress most chattiness of AsyncSerf's
   actor.

.. option:: -D, --debug

   Some DistKV methods, in particular cryptographic operations, are not
   noted for their speed. This option reduces the bit count of these
   options in order to speed them up significantly.

   It also reduces their security unacceptably. Thus, this option should
   only used while debugging.

.. option:: -c, --cfg <FILE>

   Specify a YAML configuration file.

   Data in this file override the corresponding entries in the
   ``distkv.defaults.CFG`` directory.

.. option:: -C, --conf <location=value>
   
   Set a specific configuration value.
   This option takes precedence over :option:`distkv -c`.


.. program:: distkv server

Run the DistKV server.

A DistKV server holds all data and syncs with all other DistKV servers.
You can't run :program:`distkv client` unless you have at least one running
server.

There is no separate option to set the address for clients to connect to;
use ``server.bind_default.port=57589`` (or your own port number) to change
it from the default of ``27589``, or use a configuration file.

.. option:: -h, --host <address>

   The Serf server's IP address. The default is ``localhost``.

   This option is available in the configuration file as ``server.serf.host``.

.. option:: -p, --port <port>

   The TCP port to connect to. The Serf default is 7373.

   This option is available in the configuration file as ``server.serf.port``.

.. option:: -l, --load <file>

   Pre-load the saved data from this file into the server before starting it.

   **Do not use this option with an out-of-date savefile.**

.. option:: -s, --save <file>

   Log all changes to this file. This includes the initial data.

   You can later adapt this with ``distkv client control save``

A network of servers needs to contain some data before it becomes
operational. When starting the first server, you can use an initial 

.. option:: -i, --init <value>

   Initialize the server by storing this value in the root entry.

.. option:: -e, --eval

   Evaluate the initial value, as a standard Python expression.

You can also use :program:`distkv client data set` to update this value
later.

.. option:: name

Each DistKV server requires a unique name. If you recycle a name, the old
server using it will die (unless your Serf network is disjoint – in
that case, one or both will terminate some random time after the networks
are reconnecting, and you'll get inconsistent data). So don't do that.


.. program:: distkv client

This subcommand collects all sub-subcommand which talk to a DistKV server.

.. option:: -h, --host <address>

   The address to connect to. Defaults to ``localhost``.

   This setting is also available as the ``connect.host`` configuration
   setting.

.. option:: -p, --port <port>

   The port to connect to. Defaults to 27586.

   This setting is also available as the ``connect.port`` configuration
   setting.

.. option:: -a, --auth <params>

   Parameters for authorizing this client. Use ``=file`` to load the data
   from a file, or ``method data=value…`` to provide them inline.

   The default is ``_anon``, i.e. no authorization.


.. program:: distkv client data

Basic data access.

This subcommand does not have options.


.. program:: distkv client data get

Read a DistKV value.

If you read a sub-tree recursively, be aware that the whole subtree will
be read before anything is printed. Use the ``watch --state`` subcommand
for incremental output.

.. option:: -y, --yaml

   Emit the value(s) as YAML.

   This will be the default soon.

.. option:: -v, --verbose

   Print a complete data structure, not just the value.

.. option:: -r, --recursive

   Print all entries below this entry.

.. option:: -d, --as-dict <text>

When emitting YAML recursively, the standard way is to print every entry as
an independent data structure with a ``path`` member. When you use this
option, the data is printed as a nested dictionary. The argument of this
option controls which key is used for the actual value; obviously, this
string should not occur as a path element.

The customary value to use is a single underscore, if possible.

.. option:: -m, --mindepth <integer>

   When printing recursively, start at this depth off the given path.

   The default is zero, i.e. include the entry itself.

.. option:: -M, --maxdepth <integer>

   When printing recursively, stop at this depth (inclusive).

   The default is to print the whole tree. Use ``1`` to print the entry itself
   (assuming that it has a value and you didn't use ``--mindepth=1``)
   and its immediate children.

.. option:: -c, --chain <integer>

   Include this many chain links in your output.

   Chain links tell you which DistKV server(s) last changed this entry. You
   can also use the top of the chain in the :program:`distkv client data set`
   command to ensure that the entry you're trying to change has not been
   modified since you retrieved it.

   The default is zero, i.e. do not include chain data.

.. option:: path…

   Access the entry at this location. The default is the root node,
   which usually isn't what you want.


.. program:: distkv client data set

Store a value at some DistKV position.

If you update a value, you should use :option:`--last` (preferred) or
:option:`--prev` (if you must), to ensure that no other change collides
with yours.

When adding a new entry, use :option:`--new` to ensure that you don't
accidentally overwrite something.

.. option:: -v, --value <value>

   The value to store. This option is mandatory.

.. option:: -e, --eval

   Treat the ``value`` as a Python expression, to store anything that's not a
   string.

.. option:: -c, --chain <integer>

   Include this many chain links in your reply.

   Chain links tell you which DistKV server(s) last changed this entry. You can
   also use the top of the chain in another ``set`` command, if you need to
   change this entry's value again, to ensure that it has not been
   modified since you retrieved it.

   The default is zero, i.e. do not include chain data.

.. option:: -l, --last <node> <count>

   The chain link which last modified this entry.

.. option:: -n, --new

   Use this option instead of ``--last`` or ``prev`` if the entry is new, or
   has been deleted.

.. option:: -p, --prev <value>

   The value which this entry needs to have in order to be affected.

   Try not to use this option; ``--last`` is much better.

   This value is also affected by ``--eval``.

.. option:: -y, --yaml

   Print the result of this operation as YAML data.

   This will be the default soon.

.. option:: path…

   Write to the entry at this location. The default is the root node, which
   usually isn't what you want.


.. program:: distkv client data delete

Delete the value at some DistKV position.

If you delete a value, you should use :option:`--last` (preferred) or
:option:`--prev` (if you must), to ensure that no other change collides
with your deletion.

Recursive changes only check the entry you mention on the command line.

.. option:: -c, --chain <integer>

   Include this many chain links in your reply.

   Chain links tell you which DistKV server(s) last changed this entry.

   You can use the chain link returned by this command in another ``set``
   commmand for a short time (depending on your entry deletion setup) to
   ensure that the entry has not been re-added and -deleted since you
   retrieved it. However, it's usually better to simply 

   The default is zero, i.e. do not include chain data.

.. option:: -l, --last <node> <count>

   The chain link which last modified this entry.

.. option:: -e, --eval

   Treat the ``value`` as a Python expression, to store anything that's not a
   string.

.. option:: -p, --prev <value>

   The value which this entry needs to have in order to be affected.

   Try not to use this option; ``--last`` is much better.

   This value is also affected by ``--eval``.

.. option:: -y, --yaml

   Print the result of this operation as YAML data.

   This will be the default soon.

.. option:: path…

   Write to the entry at this location. The default is the root node, which
   usually isn't what you want.


.. program:: distkv client data watch

Monitor changes to the state of an entry, or rather its subtree.

.. option:: -s, --state

   Before emitting changes, emit the current state of this subtree.

   A flag entry will be printed when this step is completed.

.. note::

   The current state may already include updates, due to DistKV's
   asynchonous nature. You should simply replace existing values.

.. option:: -c, --chain <integer>

   Include this many chain links in your replies.

   The default is zero, i.e. do not include chain data.

.. option:: -m, --msgpack

   Interpret the input as ``MsgPack`` data. XXX TODO

   The default is to use YAML. XXX TODO

.. option:: path…

   Monitor the subtree at this location. The default is the root node.


.. program:: distkv client data update

Stream a list of changes from standard input to DistKV.

.. option:: -m, --msgpack

   Interpret the input as ``MsgPack`` data. XXX TODO

   The default is to use YAML. XXX TODO

.. option:: path…

   Interpret the streamed data relative to this subtree.


.. program:: distkv client control

Control your server.  XXX TODO


.. program:: distkv client log


Control logging of changes on the server.


.. program:: distkv client log dest

Set the file to log to. The old file is closed as soon as the new file is
ready (i.e. the current state is saved).

.. option:: -i, --incremental

   The save file will only contain changes, but not the current state.

.. option:: path

   The file to write to. Note that this file is on the server.


.. program:: distkv client log save

Save the current state of the server to this file.

.. option:: path

   The file to write to. Note that this file is on the server.


.. program:: distkv client log stop

Stop logging.


.. program:: distkv client auth

Set up and change client authorization.

If you have never setup authorization, this sub-subcommands' ``--help``
options may not work. Use ``-m root`` as a workaround.  XXX TODO

.. option:: -m, --method <name>

   Affect the named method.

   DistKV supports multiple authorization methods. You need to be able to set
   up a method (add users, maybe set up codecs, …) before switching over to it.


.. program:: distkv client auth init

Set up this method.

.. option:: -s, --switch

   Actually swtich to using this method. This is the default for initial
   set-up.


.. program:: distkv client auth list

List available/configured/whatever auth methods.

XXX TODO


.. program:: distkv client auth user

Manage DistKV users.

Each authorization method has its own schema for validating users.


.. program:: distkv client auth user add

Add a new user.

Example: ``distkv client -a root auth -m password user add name=foo password=barbaz``

The actual identifier which you'd use to subsequently refer to that user is
printed when this command completes.


.. program:: distkv client auth user mod

Modify a user.

XXX TODO seems broken

.. option:: -c, --chain <int>

   XXX TODO add chain option


.. program:: distkv client auth user auth

Check that authorizing a user works.

XXX TODO seems broken

.. option:: options…

   Whichever auth options you'd normally use in ``distkv client -a TYPE …``.


.. program:: distkv client auth user get

Dump data of a user.

.. option:: -c, --chain <int>

The chain length to return, for subsequent modification.

.. option:: ident

   The user identifier, as reported by ``add``.


.. program:: distkv client auth user list

List users.

XXX TODO add verbosity


.. program:: distkv client type

Set up DistKV's type control: verify the data that clients write.

See :doc:`translator` for details.


.. program:: distkv client type get

Retrieve a type entry.

.. option:: -y, --yaml

   Print the result of this operation as YAML data.

   This will be the default soon, except for the schema file.

.. option:: -a, --all

   Save the complete record in the ``script`` file.

   XXX TODO, this is currently the default when using YAML

.. option:: -v, --verbose

   Include metadata.

.. option:: -s, --script <filename>

   Save the script to this file. Default: include in the output.

.. option:: -S, --schema <filename>

   Save the schema to this file. Default: include in the output.

.. option:: name…

   The type data to retrieve.


.. program:: distkv client type set

Add or modify a type entry.

For setting up a type, you need at least two good and one bad test value.
(If there's only one possible good value, you don't need the entry; if
there's no bad value you don't need the type check.)

Type checks accumulate: Both 'good' and 'bad' test values for "int
percent" must be accepted by "int".

Tests can use Python code, a JSON schema, or both. In the latter case the
schema is tested first.

.. option:: -y, --yaml

   Read the script file as YAML data.

   This will be the default soon, except for the schema file.

.. option:: -v, --verbose

   Print (some of) the server's return value.

.. option:: -s, --script <filename>

   Load the script from this file. Default: no script.

.. option:: -S, --schema <filename>

   Load the schema from this file. Default: no schema.

.. option:: -g <value>

   A known-good value to test the codec. It will be Python-evaluated.

.. option:: -b <value>

   A known-bad value to test the codec. It will be Python-evaluated.

.. option:: name…

   The type data to set.


.. program:: distkv client type match

Read, set or delete type matches, i.e. which part of your DistKV tree is
constricted by which type.

.. option:: -v, --verbose

   Print a complete record, not just the value.

.. option:: -t, type <name>

   The type name to use. Use multiple `--type`` options to access subtypes.
   Skip this option to display which type corresponds to the given path.

.. option:: -y, --yaml

  Emit the value(s) as YAML.

  This will be the default if verbose.

.. option:: -d, --delete

   Delete the match record instead of printing it.

.. option:: path…

   The DistKV entry to affect. Path elements '+' and '#' match exactly-one and
   one-or-more subpaths. The most specific path wins.


.. program:: distkv client codec

Set up codecs: manipulate the data that clients see, sort of like a
database view.

Codecs consist of code that encodes, i.e. converts to the user's view, and
decodes, i.e. converts to the server's storage.

Codecs cannot translate path names, or access other entries. The decoder may
modify an existing entry (or rather, use the currently-stored version when
assembling an entry's new value).

Unlike types, the codec hierarchy is strictly for convenience.


.. program:: distkv client codec get

Retrieve information about a codec, including its scripts.

.. option:: -e, --encode <file>

   The file to which to write the encoder's Python code.

.. option:: -d, --decode <file>

   The file which contains the decoder's Python code.

.. option:: -s, --script <file>

   The YAML file to which to write any other data.

   This file will also contain the scripts, if not stored otherwise.

.. option:: <name>

   The name of the codec group from which this codec should be retrieved.

.. option:: <path>

   The DistKV entry that would be affected. Path elements '+' and '#' match
   exactly-one and one-or-more subpaths. The most specific path wins.
   

.. program:: distkv client codec set

Add or modify a codec.

.. option:: -e, --encode <file>

   The file which contains the encoder's Python code.

.. option:: -d, --decode <file>

   The file which contains the decoder's Python code.

.. option:: -s, --script <file>

   The YAML file which contains any other data.
   
   Required: two arrays "in" and "out" containing tuples with before/after
   values for the decoder and encoder, respectively.

   You may store the scripts in this file, using "encode" and "decode" keys.

.. option:: <name>

   The name of the codec group to which this codec should be saved or
   modified.

.. option:: <path>

   The DistKV entry to affect. Path elements '+' and '#' match exactly-one and
   one-or-more subpaths. The most specific path wins.


.. program:: distkv client codec convert

Read, set or delete codec matches, i.e. which part of your DistKV tree is
managed by which codec. To this effect, matches are tagged by a group name.

Which codec group to apply to a given user is stored in that user's
auxiliary data as ``conv=NAME``. If no such entry is present, that user's
data are not converted.

.. option:: -v, --verbose
   
   Be more verbose. The usual.

.. option:: -c, --codec

   The codec to use on the given path. Use this option multiple times if
   the codec has a multi-level name.

.. option:: <name>

   The name of the codec group to which this codec should be saved or
   modified.

.. option:: <path>

   The DistKV entry to affect. Path elements '+' and '#' match exactly-one and
   one-or-more subpaths. The most specific path wins.


.. program:: distkv client code

Manipulate code stored in DistKV.


.. program:: distkv client code get


.. program:: distkv client code set

Store or replace Python code stored in the server.

This code will not run in the server; the purpose of these calls is to
upload code for use by client-side runners.

.. option:: -y, --yaml

   Read the script file as YAML data.

   This will be the default soon, except for the schema file.

.. option:: -v, --verbose

   Print (some of) the server's return value.

.. option:: -s, --script <filename>

   Load the code from this file. Default: Use stdin.

.. option:: -a, --async

   The code will run asynchronously, i.e. it may use ``async`` and ``await`` statements.

   You should use the ``anyio`` module for sleeping, locking etc. unless
   you *know* which async runtime is in use.

.. option:: -t, --thread

   The code will run in a worker thread.

   This option is incompatible with ``--async``.

.. option:: name…

   The path to the code to set, below ``.distkv code proc`` or whatever
   else is configured under ``codes``.

Old versions of the code continue to run; DistKV does not yet restart users.
XXX TODO


.. program:: distkv client code module

Manipulate modules stored in DistKV.

Modules are replaced immediately, but code using them is **not**
auto-restarted.


.. program:: distkv client run

Execute the code stored in DistKV.

XXX TODO XXX


.. program:: distkv pdb

This subcommand imports the debugger and then continues to process arguments.

This can be used to drop into the debugger when an exception occurs, set
breakpoints, or whatever.

.. note::

   Stepping over async function calls may or may not work. If not, your
   best bet is to set a breakpoint on the next line.
