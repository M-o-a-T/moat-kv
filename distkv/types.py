import weakref
import jsonschema

from .model import Entry
from .util import make_proc, NotGiven, singleton, Path, P
from .exceptions import ClientError, ACLError

import logging

logger = logging.getLogger(__name__)

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
    """I am a type-checking node."""

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
                code = make_proc(value.code, ("value",), self.path)
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
                        code(v, entry=None)
                except Exception:
                    raise ValueError(f"failed on good value {v!r}")
            for v in value["bad"]:
                self.parent.check_value(v)
                try:
                    if schema is not None:
                        jsonschema.validate(instance=v, schema=schema)
                    if code is not None:
                        code(v, entry=None)
                except Exception:
                    pass
                else:
                    raise ValueError(f"did not fail on {v!r}")

        await super().set(value)
        self._code = code
        self._schema = schema


TypeEntry.SUBTYPE = TypeEntry


class TypeRoot(Entry):
    """I am the root of DistKV's type hierarchy."""

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
            value.type = P(value.type)
        elif not isinstance(value.type, (Path, list, tuple)):
            raise ValueError("Type of %r is not a list" % (value.type,))
        try:
            self.metaroot["type"].follow(value.type, create=False)
        except KeyError:
            logger.exception("Type %r doesn't exist", value.type)
            raise ClientError("This type does not exist")
        # crashes if nonexistent
        await super().set(value)


MatchEntry.SUBTYPE = MatchEntry


class NodeFinder:
    """A generic object that can walk down a possibly-wildcard-equipped path.

    Example: given a path `one.two.three` and a root `bar` with subtree `bar.#.three`,
    `NodeFinder(bar).step(one).step(two).step(three)` will return the node
    at `bar.#.three` (assuming that nothing more specific hangs off `bar`).
    """

    def __init__(self, meta):
        if isinstance(meta, list):
            self.steps = meta
        else:
            self.steps = ((meta, False),)

    def step(self, name, new=False):
        steps = []
        for node, keep in self.steps:
            if name in node:
                steps.append((node[name], False))
            if name is None:
                continue
            if "+" in node:
                steps.append((node["+"], False))
            if "#" in node:
                steps.append((node["#"], True))
            if keep:
                steps.append((node, True))
            # Nodes found with '#' stay on the list
            # so that they can match multiple entries.
        # if not steps:
        #    raise KeyError(name)
        if new:
            return type(self)(steps, **self.copy_args)
        else:
            self.steps = steps
            return self

    @property
    def copy_args(self):
        return {}

    @property
    def result(self):
        for node, _keep in self.steps:
            if node._data is not NotGiven:
                return node
        return None


class ACLFinder(NodeFinder):
    """A NodeFinder which expects ACL strings as elements"""

    _block = ""

    @property
    def copy_args(self):
        return {"blocked": self._block}

    def __init__(self, acl, blocked=None):
        if isinstance(acl, ACLFinder):
            if blocked is None:
                blocked = acl._block
            acl = acl.steps
        super().__init__(acl)
        if blocked is not None:
            self._block = blocked

    def allows(self, x):
        if x in self._block:
            return False

        r = self.result
        if r is None:
            return True
        return x in r.data

    def block(self, c):
        if c not in self._block:
            self._block += c

    def check(self, x):
        if not self.allows(x):
            raise ACLError(self.result, x)


class ACLStepper(ACLFinder):
    """An ACLFinder which returns a copy of itself at every `step`."""

    def step(self, name, new=True):
        return super().step(name, new=new)


@singleton
class NullACL(ACLStepper):
    """This singleton represents an ACL that never checks anything."""

    result = "-"

    def __init__(self):  # pylint: disable=super-init-not-called
        pass

    def allows(self, x):
        return x != "a"

    def check(self, x):
        return

    def block(self, c):
        pass

    def step(self, name, new=None):
        return self


class MetaPathEntry(MetaEntry):
    def _find_node(self, entry):
        """Search for the most-specific match.

        Match entries whose values are missing are not considered.
        """
        f = NodeFinder(self)
        for n in entry.path:
            f.step(n)
        return f.result


class MatchRoot(MetaPathEntry):
    """I am the root of DistKV's type hierarchy."""

    SUBTYPE = MatchEntry

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def check_value(self, value, entry, **kv):
        """Check this value for this entry against my match hierarchy"""
        match = self._find_node(entry)
        if match is None:
            return
        typ = self.parent["type"].follow(match._data["type"])
        return typ.check_value(value, entry=entry, match=match, **kv)


class CodecEntry(Entry):
    """I am a codec."""

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
            dec = make_proc(value.decode, ("value",), self.path)
            for v, w in value["in"]:
                try:
                    r = dec(v)
                except Exception as exc:
                    raise ValueError(
                        f"failed decoder at {self.path} on {v!r} with {exc!r}"
                    ) from exc
                else:
                    if r != w:
                        raise ValueError(f"Decoding at {self.path}: {v!r} got {r!r}, not {w!r}")

        if value is not None and value.encode is not None:
            if not value["out"]:
                raise RuntimeError("Must have tests for encoding")
            enc = make_proc(value.encode, ("value",), self.path)
            for v, w in value["out"]:
                try:
                    r = enc(v)
                except Exception as exc:
                    raise ValueError(
                        f"failed encoder at {self.path} on {v!r} with {exc!r}"
                    ) from exc
                else:
                    if r != w:
                        raise ValueError(f"Encoding at {self.path}: {v!r} got {r!r}, not {w!r}")

        await super().set(value)
        self._enc = enc
        self._dec = dec


CodecEntry.SUBTYPE = CodecEntry


class CodecRoot(Entry):
    """I am the root of DistKV's codec hierarchy."""

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
        if value is not NotGiven:
            # can't run a nonexistent value through a codec

            if not hasattr(value, "codec"):
                raise ValueError("Duh? " + repr(value))
            if isinstance(value.codec, str):
                value.codec = P(value.codec)
            elif not isinstance(value.codec, (Path, list, tuple)):
                raise ValueError("Codec %r is not a list" % (value.codec,))
            try:
                self.metaroot["codec"].follow(value.codec, create=False)
            except KeyError:
                logger.warning("Codec %r does not exist (yet?)", value.codec)
                # raise ClientError("This codec does not exist")
                # don't raise here
        await super().set(value)


ConvEntry.SUBTYPE = ConvEntry


class ConvNull:
    """I am a dummy translator"""

    @staticmethod
    def enc_value(value, **_kw):
        return value

    @staticmethod
    def dec_value(value, **_kw):
        return value


ConvNull = ConvNull()


class ConvName(MetaPathEntry):
    """I am a named tree for conversion entries."""

    SUBTYPE = ConvEntry

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")

    def enc_value(self, value, entry, **kv):
        """Check this value for this entry against my converter hierarchy"""
        conv = self._find_node(entry)
        if conv is None:
            return value
        codec = self.metaroot["codec"].follow(conv._data["codec"])
        return codec.enc_value(value, entry=entry, conv=conv, **kv)

    def dec_value(self, value, entry, **kv):
        """Check this value for this entry against my converter hierarchy"""
        conv = self._find_node(entry)
        if conv is None:
            return value
        codec = self.metaroot["codec"].follow(conv._data["codec"])
        return codec.dec_value(value, entry=entry, conv=conv, **kv)


class ConvRoot(MetaEntry):
    SUBTYPE = ConvName

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")


class AclEntry(MetaPathEntry):
    """I represent an ACL.

    My data must be a string of ACLish characters.
    """

    async def set(self, value):
        if value is NotGiven:
            pass
        elif not isinstance(value, str):
            raise ValueError("ACL is not a string")
        await super().set(value)


AclEntry.SUBTYPE = AclEntry


class AclName(AclEntry):
    """I am a named tree for ACL entries."""

    SUBTYPE = AclEntry

    async def check(self, entry, typ):
        acl = self._find_node(entry)
        if acl is None:
            return None
        return typ in acl.value


class AclRoot(MetaEntry):
    SUBTYPE = AclName

    async def set(self, value):
        if value is not None:
            raise ValueError("This node can't have data.")


class DelRoot(Entry):
    SUBTYPE = None

    async def set(self, value):
        await super().set(value)
        await self.root.server.del_check(value)


class ActorRoot(Entry):
    SUBTYPE = None
    SUBTYPES = {"del": DelRoot}


# ROOT


class MetaRootEntry(Entry):  # not MetaEntry
    """I am the special node off the DistKV root that's named ``None``."""

    SUBTYPES = {
        "type": TypeRoot,
        "match": MatchRoot,
        "acl": AclRoot,
        "codec": CodecRoot,
        "conv": ConvRoot,
        "actor": ActorRoot,
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
