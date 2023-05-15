.. documentation master file, created by
   sphinx-quickstart on Sat Jan 21 19:11:14 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


===============================================
MoaT-KV: A distributed no-master key-value store
===============================================

Rationale
=========

Any kind of distributed storage is subject to the CAP theorem (also called
"Brewer's theorem"): you can't get all of (global) Consistency,
Availability, and Partition tolerance. The problem is that you do want all
three of these.

One way around this problem is to recognize that on most KV storage
systems, any given record is rarely (if ever) changed by more than one
entity at the same time. Thus, a simple gossip protocol is sufficient
for distributing data.

MoaT-KV is intended to be used in a mostly-RAM architecture. There is no
disk-based storage backend; snapshots and event logs are used to restore a
system, if necessary.

See `Protocol Overview <overview.html>` for details about MoaT-KV's
choices.

API
===

MoaT-KV offers an efficient interface to access and change data.


Status
======

MoaT-KV is in production use as the backbone of the author's home and office
automation setup.

Note that as of MoaT-KV 0.30, multi-word paths were replaced with dotted
strings. Some pieces of documentation might still reflect the old style.

.. toctree::
   :maxdepth: 2

   overview.rst
   tutorial.rst
   startup.rst
   command_line.rst
   common_protocol.rst
   client_protocol.rst
   server_protocol.rst
   auth.rst
   acls.rst
   code.rst
   model.rst
   translator.rst
   debugging.rst
   extend.rst
   related.rst

   TODO.rst
   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
