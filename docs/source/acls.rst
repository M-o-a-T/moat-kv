==============
Access control
==============

DistKV employs a two-step access control scheme.

First, you define an ACL hierarchy which controls which items may be
accessed using a particular named ACL. Then you associate that ACL
with users that shall be bound by it.


ACLs
====

An ACL entry controls these access modes:

* a: acl: retrieve the ACL flags for this node
* r: read: retrieve the data at this node
* w: write: change the data at this node
* c: create: add new data to this node
* W: check for 'c' if new node, else 'w'
* d: delete: remove the data at this node
* x: access: read specific sub-nodes below this one
* e: enumerate: list sub-nodes of this one
* n: new: create new nodes below this one

Enumeration will not check the ACL entry of each sub-node for accessibility.

ACLs can use wildcards '+' (one level) and '#' (one or more levels).
Search is depth-first; more specific keys are checked first.


Association
===========

You change a user's ACL entry by adding an "acl=ACLNAME" field to the
user's aux data. This change is instantaneous, i.e. an existing user
does not need to reconnect.


Putting it all together
=======================

TODO

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
    

The above is the server content at the end of the testcase
``tests/test_feature_convert.py::test_71_basic``, when
dumped with the command ``distkv client get -ryd_``.
