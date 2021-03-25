================
Extending DistKV
================

DistKV comes with a built-in extension mechanism for its command line,
based on Python namespaces and import hackery.

Yor extension needs to ship a ``distkv_ext.NAME`` module with a
``command.py`` or a ``client.py`` module (or both, if required). This adds
the command ``NAME`` to ``distkv`` or ``distkv client``, respectively.

This method is **not** zip-safe because it injects a ``main`` global into
the code. Fixing this is TODO.

Command line helper
===================

DistKV commands follow a standard scheme (TODO some don't yet):

* what kind of object do you want to affect
* the name of the object to affect (or create)
* [ maybe start over with a sub-object ]
* the action you want to take
* some options affecting the action, and/or
* the generic set of parameter+value options (``-v``/``-e``/``-p``)

In order to simplify implementing that, there's a couple of helper methods.

`distkv.obj.command.std_command` takes a ``click.Group`` command and
attaches a subgroup with standard add/set/delete commands to it. The
new group is returned so you can attach more commands to it if you want.

`distkv.util.attr_args` attaches DistKV's generic parameter+value options.

`distkv.util.process_args` takes a dict (usually) and the generic options'
variables (``vars_``, ``eval_``, ``path_``) and applies them.

It's the caller's job to verify that the result is sane. TODO: support
using a validation library (probably jsonschema).
