.. documentation master file, created by
   sphinx-quickstart on Sat Jan 21 19:11:14 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


===============================================
DistKV: A distributed no-master key-value store
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

DistKV is intended to be used in a mostly-RAM architecture. There is no
disk-based storage backend; snapshots and event logs are used to restore a
system, if necessary.

See `Protocol Overview <overview.html>` for details about DistKV's
choices.

API
===

DistKV offers an efficient interface to access and change data. For
compatibility, a front-end that mostly-mimics the etcd2 protocol is
planned.


Status
======

Some of the above is still wishful thinking. In particular, we don't have
an etcd2 compatibility service yet.


.. toctree::
   :maxdepth: 2

   client_protocol.rst
   server_protocol.rst
   auth.rst
   translator.rst
   history.rst

====================
 Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`glossary`
