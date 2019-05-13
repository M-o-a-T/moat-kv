import weakref
import jsonschema

from .model import Entry
from .util import make_proc, NotGiven
from .exceptions import ClientError


# TYPES


class MetaEntry(Entry):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._metaroot = self.parent._metaroot

    @property
    def metaroot(self):
        "Stores a link to the meta root because some do need it."
        return self._metaroot()


class TypeEntry(Entry):
    """I am a type-checking node.
    """

    _code = None
    _schema = None

    def check_value(self, value, entry=None, **kv):
        self.parent.check_value(value, entry=entry, **kv)
        if self._schema is not None:
            jsonschema.validate(instance=value, schema=self._schema)
        if self._code is not None:
            self._code(value, entry=entry, data=self._data, **kv)

    async def set(self, value):
        code = None
        schema = None
        if value is not NotGiven and (
            value.get("code", None) is not None or value.get("schema", None) is not None
        ):
            schema = value.get("schema", None)
            if value.get("code", None) is None:
                code = None
            else:
                code = make_proc(value.code, ("value",), *self.path)
            if len(value.good) < 2:
                raise RuntimeError("Must have check-good values")
            if not value.bad:
                raise RuntimeError("Must have check-bad values")
            for v in value["good"]:
                self.parent.check_value(v)
                try:
                    if schema is not None:
                        jsonschema.validate(instance=v, schema=schema)
                    if code is not None:
                        code(value=v, entry=None)
                except Exception as exc:
                    raise ValueError("failed on %r with %r" % (v, exc))
            for v in value["bad"]:
                self.parent.check_value(v)
                try:
                    if schema is not None:
                        jsonschema.validate(instance=v, schema=schema)
                    if code is not None:
                        code(value=v, entry=None)
                except Exception:
                    pass
                else:
                    raise ValueError("did not fail on %r" % (v,))

        await super().set(value)
        self._code = code
        self._schema = schema


TypeEntry.SUBTYPE = TypeEntry


class TypeRoot(Entry):
    """I am the root of DistKV's type hierarchy.
    """

    SUBTYPE = TypeEntry

    async def set(self, value):
        if value is not NotGiven:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry=None, **kv):
        pass


# MATCH (for types)


class MatchEntry(MetaEntry):
    """I represent a match from a path to a type.

    My data requires a "type" attribute that's a path in the "type"
    hierarchy that's next to my MatchRoot.
    """

    async def set(self, value):
        if value is NotGiven:
            pass
        elif isinstance(value.type, str):
            value.type = (value.type,)
        elif not isinstance(value.type, (list, tuple)):
            raise ValueError("Type is not a list")
        try:
            self.metaroot["type"].follow(*value.type, create=False)
        except KeyError:
            raise ClientError("This type does not exist")
        # crashes if nonexistent
        await super().set(value)


MatchEntry.SUBTYPE = MatchEntry


class MetaPathEntry(MetaEntry):
    def _find_node(self, entry):
        """Search for the most-specific match.

        Match entries whose values are missing are not considered.
        """
        p = entry.path
        checks = [(self, 0)]
        n_p = len(p)
        while checks:
            node, off = checks.pop()
            if off == n_p:
                if node._data is not NotGiven:
                    return node
                continue
            if "#" in node:
                nn = node["#"]
                pos = n_p
                while pos > off:
                    checks.append((nn, pos))
                    pos -= 1
            if "+" in node:
                checks.append((node["+"], off + 1))
            if p[off] in node:
                checks.append((node[p[off]], off + 1))
        return None


class MatchRoot(MetaPathEntry):
    """I am the root of DistKV's type hierarchy.
    """

    SUBTYPE = MatchEntry

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry, **kv):
        """Check this value for this entry against my match hierarchy"""
        match = self._find_node(entry)
        if match is None:
            return
        typ = self.parent["type"].follow(*match._data["type"])
        return typ.check_value(value, entry=entry, match=match, **kv)


class CodecEntry(Entry):
    """I am a codec.
    """

    _enc = None
    _dec = None

    def enc_value(self, value, entry=None, **kv):
        if self._enc is not None:
            try:
                value = self._enc(value, entry=entry, data=self._data, **kv)
            except TypeError:
                if value is not None:
                    raise
        return value

    def dec_value(self, value, entry=None, **kv):
        if self._dec is not None:
            try:
                value = self._dec(value, entry=entry, data=self._data, **kv)
            except TypeError:
                if value is not None:
                    raise
        return value

    async def set(self, value):
        enc = None
        dec = None
        if value is not None and value.decode is not None:
            if not value["in"]:
                raise RuntimeError("Must have tests for decoding")
            dec = make_proc(value.decode, ("value",), *self.path)
            for v, w in value["in"]:
                try:
                    r = dec(v)
                except Exception as exc:
                    raise ValueError("failed decoder on %r with %r" % (v, exc))
                else:
                    if r != w:
                        raise ValueError("Decoding %r got %r, not %r" % (v, r, w))

        if value is not None and value.encode is not None:
            if not value["out"]:
                raise RuntimeError("Must have tests for encoding")
            enc = make_proc(value.encode, ("value",), *self.path)
            for v, w in value["out"]:
                try:
                    r = enc(v)
                except Exception as exc:
                    raise ValueError("failed encoder on %r with %r" % (v, exc))
                else:
                    if r != w:
                        raise ValueError("Encoding %r got %r, not %r" % (v, r, w))

        await super().set(value)
        self._enc = enc
        self._dec = dec


CodecEntry.SUBTYPE = CodecEntry


class CodecRoot(Entry):
    """I am the root of DistKV's codec hierarchy.
    """

    SUBTYPE = CodecEntry

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry=None, **kv):
        pass


# CONV (for codecs)


class ConvEntry(MetaEntry):
    """I represent a converter from a path to a type.

    My data requires a "codec" attribute that's a path in the "codecs"
    hierarchy that's next to my Root.
    """

    async def set(self, value):
        if isinstance(value.codec, str):
            value.codec = (value.codec,)
        elif not isinstance(value.codec, (list, tuple)):
            raise ValueError("Codec is not a string or list")
        try:
            self.metaroot["codec"].follow(*value.codec, create=False)
        except KeyError:
            raise ClientError("This codec does not exist")
        # crashes if nonexistent
        await super().set(value)


ConvEntry.SUBTYPE = ConvEntry


class ConvNull:
    """I am a dummy translator"""

    @staticmethod
    def enc_value(value, **k):
        return value

    @staticmethod
    def dec_value(value, **k):
        return value


ConvNull = ConvNull()


class ConvName(MetaPathEntry):
    """I am a named tree for conversion entries.
    """

    SUBTYPE = ConvEntry

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def enc_value(self, value, entry, **kv):
        """Check this value for this entry against my converter hierarchy"""
        conv = self._find_node(entry)
        if conv is None:
            return value
        codec = self.metaroot["codec"].follow(*conv._data["codec"])
        return codec.enc_value(value, entry=entry, conv=conv, **kv)

    def dec_value(self, value, entry, **kv):
        """Check this value for this entry against my converter hierarchy"""
        conv = self._find_node(entry)
        if conv is None:
            return value
        codec = self.metaroot["codec"].follow(*conv._data["codec"])
        return codec.dec_value(value, entry=entry, conv=conv, **kv)


class ConvRoot(MetaEntry):
    SUBTYPE = ConvName

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")


class CoreRoot(Entry):
    SUBTYPE = None

    async def set(self, value):
        await super().set(value)
        await self.root.server.core_check(value)


# ROOT


class MetaRootEntry(Entry):  # not MetaEntry
    """I am the special node off the DistKV root that's named ``None``."""

    SUBTYPES = {
        "type": TypeRoot,
        "match": MatchRoot,
        "codec": CodecRoot,
        "conv": ConvRoot,
        "core": CoreRoot,
    }

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._metaroot = weakref.ref(self)


Entry.SUBTYPES[None] = MetaRootEntry


class RootEntry(Entry):
    """I am the root of the DistKV data tree."""

    def __init__(self, server, *a, **k):
        super().__init__("ROOT", None, *a, **k)
        self._server = weakref.ref(server)

    @property
    def server(self):
        return self._server()

    SUBTYPES = {None: MetaRootEntry}


def check_type(value, *path):
    """Verify that this type works."""
