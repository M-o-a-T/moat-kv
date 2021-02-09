============================
Plugins and related software
============================

Additions to this list will be appreciated!


Home Assistant
==============

`Home Assistant`__ is a front-end for home automation. (Actually it's more
than that, but ``distkv`` uses it as a front-end and prefers to do the
automation part itself.)

`distkv-hass <https://github.com/M-o-a-T/disthass>`__ documents how to
connect Home Assistant to the ``distkv`` system and helps with creating the
data structures that teach Home Assistant about distkv-controlled sensors
and actors.


KNX
===

KNX is a serial bus for building control.

`knxd <https://github.com/knxd/knxd/>`__ is a server commonly used to talk to KNX interfaces.

`xknx <https://github.com/XKNX/xknx>`__ is a Python package you can use to talk to ``knxd``.

`distkv-knx <https://github.com/M-o-a-T/distknx>`__ connects values stored
in distkv to devices on the KNX bus.

 
1wire
=====

`1wire <https://en.wikipedia.org/wiki/1-Wire>`__ is a two- or three-wire
bus (one signal wire, somewhat-optional 5V power, ground) that is
frequently used to connect inexpensive sensors and actors to a computer.

`OWFS <https://www.owfs.org/>`__ is the server commonly used on Linux
systems to talk to 1wire.

`asyncowfs <https://github.com/M-o-a-T/asyncowfs>`__ is a Python package
that provides a high-level object-oriented async interface to OWFS.

`distkv-owfs <https://github.com/M-o-a-T/distknx>`__ uses ``asyncowfs`` to
connect values stored in ``distkv`` to attributes if 1wire devices.


Inventory Management
====================

`distkv-inv <https://github.com/M-o-a-T/distinv>`__ is a command-line
extension that simplifies storage of hosts, networks and
cables in ``distkv``. It contains templating code so you can auto-create
the configuration for your router (if it's text instead of some binary
format).


Akumuli
=======

`Akumuli <https://akumuli.org/>`__ is a time series database.

`asyncakumuli <https://github.com/M-o-a-T/asyncakumuli>`__ is a Python package
that provides an async interface to Akumuli.

`distkv-akumuli <https://github.com/M-o-a-T/distakumuli>`__ implements a
background task that monitors values stored in ``distkv`` and mirrors them
into Akumuli, thus saving their history.


Binary I/O
==========

`asyncgpio <https://github.com/M-o-a-T/asyncgpio>`__ is a Python package
that provides structured access to your computer's I/O ports.

`distkv-gpio <https://github.com/M-o-a-T/distgpio>`__ contains code that
mirrors a binary value stored in ``distkv`` to a GPIO pin and vice versa.


Wago I/O controllers
====================

The German company `WAGO Kontakttechnik <https://www.wago.com>`__ makes the
``750-*`` line of extensible rugged controllers with various modules.


