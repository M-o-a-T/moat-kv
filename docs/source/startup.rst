Starting DistKV
===============

DistKV is generally started by a systemd service.

You can start a DistKV service in one of three ways:

* Slave, i.e. no persistent storage.

* Master, i.e. always starts with persistent storage.

* Hybrid, i.e. tries to start from the network but loads persistent data
  when that fails.

Slave mode is used when ``/var/lib/distkv`` does not exist.

Master mode is used when ``MODE=master`` is set in ``/etc/distkv.env``.

Hybrid mode is used when neither of the above is true.

Master and hybrid mode use a systemd timer unit to rotate the logs.
The rotation period is specified in ``distkv.timer`` and can be overridden
by copying that file to ``/etc/systemd/system`` and editing it.

The period for starting a new log is specified by the DATE format variable
in ``/etc/distkv.env``; the default is "daily", i.e. ``%Y-%m-%d``. If you
change this format, you must only use strictly incrementing values: month
numbers and Y-M-D format is OK but M-D-Y or month/day names is not. See
``man date`` for details. You may use slashes in the format to subdivide
the files further.

