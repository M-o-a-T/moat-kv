=================================
Verifying and Translating Entries
=================================

++++++++++++
Verification
++++++++++++

Your application may require consistency guarantees. Instead of committing
fraud when a transaction in your bookkeeping system doesn't add up to zero,
you might want to add a verification step to make sure that that doesn't
happen in the first place. More prosaically, the statement "The door is
locked" is either True or False. (However, you always should be prepared
for an answer of "No idea", aka ``None``. That's not avoidable.)


Types
=====

Type entries may contain a ``schema`` attribute with a JSON Schema that
verifies the data. They also may contain a ``code`` attribute which forms
the body of a validation procedure. The variable ``value`` contains the
value in question.

Type entries are hierarchic: An ("int","percent") type is first validated
against (None,"type","int"), then against (None,"type","int","percent").

Type checkers cannot modify data.

A value of ``None`` may represent a deleted entry and thus is never
typechecked.

Type check entries *must* be accompanied by "good" and "bad" values, which
must be non-empty arrays of values which pass or fail this type check. For
subordinate types, both kinds must pass the supertype check: if you
add a type "float percentage", the ``bad`` list may contain values like ``-1.2`` or
``123.45``, but not ``"hello"``.

Beware that restricting an existing type is dangerous. The DistKV server
does not verify that all existing entries verify correctly.
In pedantic mode, your network may no longer load its data or converge.


Matches
=======

The (None,"match") hierarchy mirrors the actual object tree, except that
wildcards are allowed:

* "#"

  matches any number of levels

* "+"

  matches exactly one level

This matches MQTT's behavior.

Unlike MQTT, there may be more than one "#" wildcard.

Be aware that adding or modifying matches to existing entries is dangerous.
The DistKV server does not verify that all existing entries verify correctly.
In pedantic mode, your network may no longer load its data or converge.


Putting it all together
=======================

Given the following structure, values stored at ("foo", anything, "bar")
must be integers. The DistKV content of testcase
``tests/test_feature_typecheck.py::test_72_cmd`` looks like this, when
dumped with the command ``get -ryd_``::

    _: 123
    null:
      match:
        foo:
          +:
            bar:
              _:
                type:
                - int
                - percent
      type:
        int:
          _:
            bad: [none, "foo"]
            code: 'if not isinstance(value,int): raise ValueError(''not an int'')'
            good: [0,2]
          percent:
            _:
              bad: [-1,555]
              code: 'if not 0<=value<=100: raise ValueError(''not a percentage'')
    
                '
              good: [0,100,50]
    foo:
      dud:
        bar:
          _: 55



+++++++++++
Translation
+++++++++++

Sometimes, clients need special treatment. For instance, an IoT-MQTT message
that reports turning on a light might send "ON" to topic
``/home/state/bath/light``, while what you'd really like to do is to change
the Boolean ``state`` attribute of ``home.bath.lights``. Or maybe the value
is a percentage and you'd like to ensure that the stored value is 0.5
instead of "50%", and that no rogue client can set it to -20 or "gotcha".

To ensure this, DistKV employs a two-level type mechanism.

* "type" entries describe the type of entry ("this is an integer between 0
  and 42").

* "match" entries describe the path position to which that type applies

In addition, a similar mechanism may be used to convert clients' values to
DistKV entries and back.

* "codec" entries describe distinct converters ("50%" => 0.5; "ON" => 'set
  the entry's "state" property to ``True``')

* "map" entries are activated per client (via command, or controlled by its
  login) and describe the path position to which a codec applies

All of these are stored below the global (``None``) top-level path.


Codecs
======

Codec entries contain ``decode`` and ``encode`` attributes which form the
bodies of procedures that rewrite external data to DistKV values and vice
versa, respectively, using the ``value`` parameter as input. The ``decode``
procedure gets an additional ``prev`` variable which contains the old
value. That value **must not** be modified; create a copy or (preferably)
use :func:`distkv.util.combine_dict` to assemble the result.

Codecs may be named hierarchically for convenience; if you want to
call the "parent" codec, put the common code in a module and import that.

Codecs also require "in" and "out" attributes, each of which must contain a list
of 2-tuples with that conversion's source value and its result. "in"
corresponds to decoding, "out" to encoding – much like Python's binary
codecs.

.. autofunction:: distkv.util.combine_dict

Converters
==========

While the ``(None,"map")`` contains a single mapping, ``(None,"conv")``
contains an additional single level of names. A mapping must be applied to
a user before it is used. This change is instantaneous, i.e. an existing
user does not need to reconnect.

Below that, converter naming works like that for mappings. Of course, the
pointing attribute is named ``codec`` instead of ``type``.


Putting it all together
=======================

Given the following data structure, the user "conv" will only be able to
write stringified integers under keys below the "inty" key, which will be
stored as integers::


    null:
      auth:
        _:
          current: _test
        _test:
          user:
            con:
              _:
                _aux:
                  conv: foo
            std:
              _:
                _aux: {}
      codec:
        int:
          _:
            decode: assert isinstance(value,str); return int(value)
            encode: return str(value)
            in:
            - [ '1', 1 ]
            - [ '2', 2 ]
            - [ '3', 3 ]
            out:
            - [ 1, '1' ]
            - [ 2, '2' ]
            - [ -3, '-3' ]
      conv:
        foo:
          inty:
            '#':
              _:
                codec:
                - int
    inty:
      _: hello
      ten:
        _: 10
      yep:
        yepyepyep:
          _: 13
          yep:
            _: 99
    

The above is the content at the enf of the testcase
``tests/test_feature_convert.py::test_71_basic``.
