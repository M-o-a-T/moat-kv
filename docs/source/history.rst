Release history
===============

.. currentmodule:: moat.kv

.. towncrier release notes start

Migration from DistKV
=====================

As of 2023-05, the protocol is unchanged.

To re-use the DistKV data as-is, copy ``/etc/distkv.cfg``  to
``/etc/moat/moat.cfg``. Then edit ``moat.cfg``:

* Move the ``logging:`` entry to the bottom if it isn't there already
* Indent everything above that entry two spaces
* Insert a line ``kv:`` as the first line of the file (not indented!)

Then, add these items as appropriate::

    kv:
      inv:
        prefix: !P :.distkv.inventory
      config:
        prefix: !P :.distkv.config
      errors:
        prefix: !P :.distkv.errors
      codes:
        prefix: !P :.distkv.code.proc
      modules:
        prefix: !P :.distkv.code.module
      runner:
        prefix: !P :.distkv.run
        state: !P :.distkv.state
    
