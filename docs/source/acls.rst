==============
Access control
==============

MoaT-KV employs a two-step access control scheme.

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
* d: delete: remove the data at this node
* x: access: read specific sub-nodes below this one
* e: enumerate: list sub-nodes of this one
* n: new: create new nodes below this one

In the MoaT-KV sources you'll also encounter these modes in calls to
``follow_acl`` (i.e. these flags can be checked for but you cannot set
them):

* W: check 'c' if the node is new or has no data, else 'w'

ACLs can use wildcards '+' (one level) and '#' (one or more levels).
Search is depth-first; more specific keys are checked first.


Association
===========

You change a user's ACL entry by adding an "acl=ACLNAME" field to the
user's aux data. The user is affected as soon as they log back in.

Updated ACL records are effective immediately.


Putting it all together
=======================

Given the following data structure, the user "aclix" will only be able to
write initial data to ``one`` and ``one two``. They can also read the data
back. However, any other access is not possible::

    null:
      auth:
        _:
          current: _test
        _test:
          user:
            aclix:
              _:
                _aux:
                  acl: foo
            std:
              _:
                _aux: {}
      acl:
        foo:
          one:
            _: rxnc
            two:
              _: rc
    one:
      _: 10
      two:
        _: 11
    

The above is the server content at the end of the testcase
``tests/test_feature_acls.py::test_81_basic``, when
dumped with the command ``moat kv : get -rd_``.

