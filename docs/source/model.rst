==========
Data Model
==========

This section documents some of DistKV's server-internal classes.


.. automodule:: distkv.model
   :members:

Helper methods and classes
--------------------------

.. automodule:: distkv.util
   :members:

.. py:data:: distkv.util.NotGiven

   This object marks the absence of information where simply not using the
   data element or keyword at all would be inconvenient.

   For instance, in ``def fn(value=NotGiven, **kw)`` you'd need to test
   ``'value'  in kw``, or use an exception. The problem is that this would
   not show up in the function's signature.

   With ``NotGiven`` you can simply test ``value is`` (or ``is not``) ``NotGiven``.
