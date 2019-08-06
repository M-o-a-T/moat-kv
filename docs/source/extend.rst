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
