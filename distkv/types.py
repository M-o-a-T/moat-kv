import weakref

from .model import Entry
from .util import make_proc
from .exceptions import ClientError


# TYPES

class MetaEntry(Entry):
    def __init__(self, *a,**k):
        super().__init__(*a,**k)
        self._metaroot = self.parent._metaroot

    @property
    def metaroot(self):
        "Stores a link to the meta root because some do need it."
        return self._metaroot()


class TypeEntry(Entry):
    """I am a type-checking node.
    """

    _code = None

    def check_value(self, value, entry=None, **kv):
        self.parent.check_value(value, entry=entry, **kv)
        if self._code is not None:
            self._code(value, entry=entry, data=self._data, **kv)

    def _set(self, value):
        code = None
        if value is not None and value.code is not None:
            code = make_proc(value.code, ("value",), *self.path)
            if len(value.good) < 2:
                raise RuntimeError("Must have check-good values")
            if not value.bad:
                raise RuntimeError("Must have check-bad values")
            for v in value['good']:
                self.parent.check_value(v)
                try:
                    code(value=v, entry=None)
                except Exception as exc:
                    raise ValueError("failed on %r with %r" % (v, exc))
            for v in value['bad']:
                self.parent.check_value(v)
                try:
                    code(value=v, entry=None)
                except Exception:
                    pass
                else:
                    raise ValueError("did not fail on %r" % (v,))

        super()._set(value)
        self._code = code

TypeEntry.SUBTYPE = TypeEntry

class TypeRoot(Entry):
    """I am the root of DistKV's type hierarchy.
    """
    SUBTYPE = TypeEntry

    def _set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry=None, **kv):
        pass


# MATCH (for types)

class MatchEntry(MetaEntry):
    """I represent a match form a path to a type.

    My data requires a "type" attribute that's a path in the "type"
    hierarchy that's next to my MatchRoot.
    """
    def _set(self, value):
        if isinstance(value.type, str):
            value.type = (value.type,)
        elif not isinstance(value.type, (list,tuple)):
            raise ValueError("Type is not a list")
        try:
            typ = self.metaroot['type'].follow(*value.type, create=False)
        except KeyError:
            raise ClientError("This type does not exist")
        # crashes if nonexistent
        super()._set(value)


MatchEntry.SUBTYPE = MatchEntry

class MatchRoot(MetaEntry):
    """I am the root of DistKV's type hierarchy.
    """
    SUBTYPE = MatchEntry

    def _set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry, **kv):
        """Check this value for this entry against my match hierarchy"""
        p = entry.path
        checks = [(self,0)]
        match = self._find_node(entry)
        if match is None:
            return
        typ = self.parent['type'].follow(*match._data['type'])
        return typ.check_value(value, entry=entry, match=match, **kv)

    def _find_node(self, entry):
        """Search for the most-specific match.

        Match entries whose values are missing are not considered.
        """
        p = entry.path
        checks = [(self,0)]
        n_p = len(p)
        while checks:
            node,off = checks.pop()
            if off == n_p:
                if node._data is not None:
                    return node
                continue
            if '#' in node:
                nn = node['#']
                pos = n_p
                while pos > off:
                    checks.append((nn,pos))
                    pos -= 1
            if '+' in node:
                checks.append((node['+'],off+1))
            if p[off] in node:
                checks.append((node[p[off]],off+1))
        return None


# ROOT

class MetaRootEntry(Entry):  # not MetaEntry
    """I am the special node off the DistKV root that's named wht "None"."""
    SUBTYPES = {'type': TypeRoot, 'match': MatchRoot}

    def __init__(self, *a,**k):
        super().__init__(*a,**k)
        self._metaroot = weakref.ref(self)


Entry.SUBTYPES[None] = MetaRootEntry

class RootEntry(Entry):
    """I am the root of the DistKV data tree."""

    def __init__(self, *a,**k):
        super().__init__("ROOT", None, *a,**k)

    SUBTYPES = {None: MetaRootEntry}


def check_type(value, *path):
    """Verify that this type works."""

