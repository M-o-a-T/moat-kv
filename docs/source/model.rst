==========
Data Model
==========

This section documents some of MoaT-KV's server-internal classes.


.. automodule:: moat.kv.model
   :members:

ACLs
----

ACL checks are performed by :class:`~moat.kv.types.ACLFinder`. This class
collects all relevant ACL entries for any given (sub)path, sorted by
depth-first specificty. This basically means that you collect all ACLs
that could possibly match a path and sort them; the ``+`` and ``#``
wildcards get sorted last. Then the system picks the first entry that
actually has a value.

This basically means that if you have a path ``a b c d e f g`` and ACLs ``a
b # g`` and ``a # d e f g``, the first ACL will match because ``b`` is
more specific than ``#``, even though the second ACL is longer and thus
could be regarded as being more specific. However, the current rule is more
stable when used with complex ACLs and thus more secure.

.. autoclass:: moat.kv.types.ACLFinder
   :members:

Helper methods and classes
--------------------------

.. autoclass:: moat.kv.util.MsgWriter
   :members:

.. automodule:: moat.kv.util
   :members:

.. py:data:: moat.kv.util.NotGiven

   This object marks the absence of information where simply not using the
   data element or keyword at all would be inconvenient.

   For instance, in ``def fn(value=NotGiven, **kw)`` you'd need to test
   ``'value'  in kw``, or use an exception. The problem is that this would
   not show up in the function's signature.

   With ``NotGiven`` you can simply test ``value is`` (or ``is not``) ``NotGiven``.

.. automodule:: moat.kv.runner
   :members:

.. automodule:: moat.kv.actor
   :members:
