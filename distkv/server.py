# Local server
from __future__ import annotations

import os
import io
import signal
import time
import trio  # signaling
import anyio

try:
    from trio import BrokenResourceError as trioBrokenResourceError
    from trio import Cancelled as trioCancelled
except ImportError:

    class trioBrokenResourceError(Exception):
        pass

    class trioCancelled(BaseException):
        pass


try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager
from typing import Any, Dict
from range_set import RangeSet
from functools import partial
from asyncactor import Actor, GoodNodeEvent, RecoverEvent, RawMsgEvent
from asyncactor import TagEvent, UntagEvent, DetagEvent
from asyncactor.backend import get_transport
from pprint import pformat
from collections.abc import Mapping

from .model import NodeEvent, Node, Watcher, UpdateEvent, NodeSet
from .types import RootEntry, ConvNull, NullACL, ACLFinder, ACLStepper
from .actor.deletor import DeleteActor
from .default import CFG
from .codec import packer, unpacker, stream_unpacker
from .backend import get_backend
from .util import (
    attrdict,
    PathShortener,
    PathLongener,
    MsgWriter,
    MsgReader,
    combine_dict,
    drop_dict,
    create_tcp_server,
    gen_ssl,
    num2byte,
    byte2num,
    NotGiven,
    ValueEvent,
    Path,
    P,
)
from .exceptions import (
    ClientError,
    ClientChainError,
    NoAuthError,
    CancelledError,
    ACLError,
    ServerError,
    ServerClosedError,
    ServerConnectionError,
)
from . import client as distkv_client  # needs to be mock-able
from . import _version_tuple

import logging

logger = logging.getLogger(__name__)


_client_nr = 0

SERF_MAXLEN = 450
SERF_LEN_DELTA = 15


def max_n(a, b):
    if a is None:
        return b
    elif b is None:
        return a
    elif a < b:
        return b
    else:
        return a


def cmp_n(a, b):
    if a is None:
        a = -1
    if b is None:
        b = -1
    return b - a


class StreamCommand:
    """Represent the execution of a streamed command.

    Implement the actual command by overriding ``run``.
    Read the next input line by reading ``in_q``.

    This auto-detects whether the client sends multiple lines, by closing
    the incoming channel if there's no state=start in the command.

    Selection of outgoing multiline-or-not must be done beforehand,
    by setting ``.multiline``: either statically in a subclass, or
    overriding ``__call__``.
    """

    multiline = False
    send_q = None
    _scope = None
    end_msg = None

    def __new__(cls, client, msg):
        if cls is StreamCommand:
            cls = globals()["SCmd_" + msg.action]  # pylint: disable=self-cls-assignment
            return cls(client, msg)
        else:
            return object.__new__(cls)

    def __init__(self, client, msg):
        self.client = client
        self.msg = msg
        self.seq = msg.seq
        self.in_q = anyio.create_queue(1)
        self.client.in_stream[self.seq] = self

    async def received(self, msg):
        """Receive another message from the client"""

        s = msg.get("state", "")
        err = msg.get("error", None)
        if err:
            await self.in_q.put(msg)
        if s == "end":
            self.end_msg = msg
            await self.aclose()
        elif not err:
            await self.in_q.put(msg)

    async def aclose(self):
        self.client.in_stream.pop(self.seq, None)
        if self.in_q is not None:
            await self.in_q.put(None)
            self.in_q = None

    async def recv(self):
        msg = await self.in_q.get()
        if msg is None:
            raise ServerClosedError

        if "error" in msg:
            raise ClientError(msg.error)
        return msg

    async def send(self, **msg):
        """Send a message to the client.

        TODO add a rate limit
        """
        msg["seq"] = self.seq
        if not self.multiline:
            if self.multiline is None:
                raise RuntimeError("Non-Multiline tried to send twice")
            self.multiline = None
        elif self.multiline == -1:
            raise RuntimeError("Can't explicitly send in simple interaction")
        try:
            await self.client.send(msg)
        except trioBrokenResourceError:
            self.client.logger.info("OERR %d", self.client._client_nr)

    async def __call__(self, **kw):
        msg = self.msg
        if msg.get("state") != "start":
            # single message
            if self.in_q is not None:
                await self.in_q.put(None)
                self.in_q = None

        if self.multiline > 0:
            await self.send(state="start")
            try:
                res = await self.run(**kw)
                if res is not None:
                    await self.send(**res)
            except Exception as exc:
                if not isinstance(exc, CancelledError):
                    self.client.logger.exception("ERS%d %r", self.client._client_nr, self.msg)
                await self.send(error=repr(exc))
            finally:
                async with anyio.move_on_after(2, shield=True):
                    await self.send(state="end")

        else:
            res = await self.run(**kw)
            if res is None:
                if self.multiline is None:
                    return
                res = {}
            if self.multiline is None:
                raise RuntimeError("Can't explicitly send in single-line reply")
            if self.multiline < 0:
                return res
            res["seq"] = self.seq
            await self.send(**res)

    async def run(self):
        raise RuntimeError("Do implement me!")


class SingleMixin:
    """This is a mix-in that transforms a StreamCommand into something that
    doesn't."""

    multiline = -1

    async def __call__(self, **kw):
        await self.aclose()
        return await super().__call__(**kw)


class SCmd_auth(StreamCommand):
    """
    Perform user authorization.

    root: sub-root directory
    typ: auth method (root)
    ident: user identifier (*)

    plus any other data the client-side auth object sends

    This call cannot be used to re-authenticate. The code will go
    through the motions but not actually do anything, thus you can
    non-destructively test an updated authorization.
    """

    multiline = True
    noAuth = True

    async def run(self):
        from .auth import loader

        msg = self.msg
        client = self.client

        if client._user is not None:
            await client._user.auth_sub(msg)
            return

        root = msg.get("root", Path())
        auth = client.root.follow(root + (None, "auth"), nulls_ok=2, create=False)
        if client.user is None:
            a = auth.data["current"]
            if msg.typ != a and client.user is None:
                raise RuntimeError("Wrong auth type", a)

        data = auth.follow(Path(msg.typ, "user", msg.ident), create=False)

        cls = loader(msg.typ, "user", server=True)
        user = cls.load(data)
        client._user = user
        try:
            await user.auth(self, msg)

            if client.user is None:
                client._chroot(root)
                client.user = user

                client.conv = user.aux_conv(data, client.root)
                client.acl = user.aux_acl(data, client.root)
        finally:
            client._user = None


class SCmd_auth_list(StreamCommand):
    """
    List auth data.

    root: sub-root directory
    typ: auth method (root)
    kind: type of data to read('user')
    ident: user identifier (foo) (if missing: return all)
    """

    multiline = True

    async def send_one(self, data, nchain=-1):
        from .auth import loader

        typ, kind, ident = data.path[-3:]
        cls = loader(typ, kind, server=True, make=False)
        user = cls.load(data)
        res = user.info()
        res["typ"] = typ
        res["kind"] = kind
        res["ident"] = ident
        if data.chain is not None and nchain != 0:
            res["chain"] = data.chain.serialize(nchain=nchain)

        await self.send(**res)

    async def run(self):
        msg = self.msg
        client = self.client
        if not client.user.can_auth_read:
            raise RuntimeError("Not allowed")

        nchain = msg.get("nchain", 0)
        root = msg.get("root", ())
        if root and not self.client.user.is_super_root:
            raise RuntimeError("Cannot read tenant users")
        kind = msg.get("kind", "user")

        auth = client.root.follow(root + (None, "auth"), nulls_ok=2, create=False)
        if "ident" in msg:
            data = auth.follow(Path(msg.typ, kind, msg.ident), create=False)
            await self.send_one(data, nchain=nchain)

        else:
            d = auth.follow(Path(msg.typ, kind), create=False)
            for data in d.values():
                await self.send_one(data, nchain=nchain)

    async def __call__(self, **kw):
        # simplify for single-value result
        msg = self.msg
        self.multiline = "ident" not in msg
        return await super().__call__(**kw)


class SCmd_auth_get(StreamCommand):
    """
    Read auth data.

    root: sub-root directory
    typ: auth method (root)
    kind: type of data to read('user')
    ident: user identifier (foo)
    chain: change history

    plus any other data the client-side manager object sends
    """

    multiline = False

    async def run(self):
        from .auth import loader

        msg = self.msg
        client = self.client
        if not client.user.can_auth_read:
            raise RuntimeError("Not allowed")

        root = msg.get("root", ())
        if root and not self.client.user.is_super_root:
            raise RuntimeError("Cannot read tenant users")
        kind = msg.get("kind", "user")

        auth = client.root.follow(root + (None, "auth"), nulls_ok=2, create=False)
        data = auth.follow(Path(msg.typ, kind, msg.ident), create=False)
        cls = loader(msg.typ, kind, server=True, make=False)
        user = cls.load(data)

        res = user.info()
        nchain = msg.get("nchain", 0)
        if nchain:
            res["chain"] = data.chain.serialize(nchain=nchain)
        return res


class SCmd_auth_set(StreamCommand):
    """
    Write auth data.

    root: sub-root directory
    typ: auth method (root)
    kind: type of data to read('user')
    ident: user identifier (foo)
    chain: change history

    plus any other data the client sends
    """

    multiline = True

    async def run(self):
        from .auth import loader

        msg = self.msg
        client = self.client
        if not client.user.can_auth_write:
            raise RuntimeError("Not allowed")

        root = msg.get("root", ())
        if root and not self.client.user.is_super_root:
            raise RuntimeError("Cannot write tenant users")
        kind = msg.get("kind", "user")

        cls = loader(msg.typ, kind, server=True, make=True)
        auth = client.root.follow(root + (None, "auth"), nulls_ok=2, create=True)

        data = auth.follow(Path(msg.typ, kind, msg.ident), create=True)
        user = cls.load(data)
        val = user.save()
        val = drop_dict(val, msg.pop("drop", ()))
        val = combine_dict(msg, val)

        user = await cls.recv(self, val)
        msg.value = user.save()
        msg.path = (*root, None, "auth", msg.typ, kind, user.ident)
        return await client.cmd_set_value(msg, _nulls_ok=True)


class SCmd_get_tree(StreamCommand):
    """
    Get a subtree.

    path: position to start to enumerate.
    min_depth: tree depth at which to start returning results. Default 0=path location.
    max_depth: tree depth at which to not go deeper. Default +inf=everything.
    nchain: number of change chain entries to return. Default 0=don't send chain data.

    The returned data is PathShortened.
    """

    multiline = True

    async def run(self, root=None):  # pylint: disable=arguments-differ
        msg = self.msg
        client = self.client

        if root is None:
            root = client.root
            entry, acl = root.follow_acl(
                msg.path, create=False, nulls_ok=client.nulls_ok, acl=client.acl, acl_key="e"
            )
        else:
            entry, _ = root.follow_acl(msg.path, create=False, nulls_ok=client.nulls_ok)
            acl = NullACL

        kw = {}
        nchain = msg.get("nchain", 0)
        ps = PathShortener(entry.path)
        max_depth = msg.get("max_depth", None)
        empty = msg.get("add_empty", False)
        conv = client.conv

        if max_depth is not None:
            kw["max_depth"] = max_depth
        min_depth = msg.get("min_depth", None)
        if min_depth is not None:
            kw["min_depth"] = min_depth
        kw["full"] = empty

        async def send_sub(entry, acl):
            if entry.data is NotGiven and not empty:
                return
            res = entry.serialize(chop_path=client._chop_path, nchain=nchain, conv=conv)
            if not acl.allows("r"):
                res.pop("value", None)
            ps(res)
            await self.send(**res)

            if not acl.allows("e"):
                raise StopAsyncIteration
            if not acl.allows("x"):
                acl.block("r")

        await entry.walk(send_sub, acl=acl, **kw)


class SCmd_get_tree_internal(SCmd_get_tree):
    """Get a subtree (internal data)."""

    async def run(self):  # pylint: disable=arguments-differ
        return await super().run(root=self.client.metaroot)


class SCmd_watch(StreamCommand):
    """
    Monitor a subtree for changes.
    If ``state`` is set, dump the initial state before reporting them.

    path: position to start to monitor.
    nchain: number of change chain entries to return. Default 0=don't send chain data.
    state: flag whether to send the current subtree before reporting changes. Default False.

    The returned data is PathShortened.
    The current state dump may not be consistent; always process changes.
    """

    multiline = True

    async def run(self):
        msg = self.msg
        client = self.client
        conv = client.conv
        entry, acl = client.root.follow_acl(
            msg.path, acl=client.acl, acl_key="x", create=True, nulls_ok=client.nulls_ok
        )
        nchain = msg.get("nchain", 0)
        max_depth = msg.get("max_depth", -1)
        min_depth = msg.get("min_depth", 0)
        empty = msg.get("add_empty", False)

        async with Watcher(entry) as watcher:
            async with anyio.create_task_group() as tg:
                tock = client.server.tock
                shorter = PathShortener(entry.path)
                if msg.get("fetch", False):

                    async def orig_state():
                        kv = {"max_depth": max_depth, "min_depth": min_depth}

                        async def worker(entry, acl):
                            if entry.data is NotGiven and not empty:
                                return
                            if entry.tock < tock:
                                res = entry.serialize(
                                    chop_path=client._chop_path, nchain=nchain, conv=conv
                                )
                                shorter(res)
                                if not acl.allows("r"):
                                    res.pop("value", None)
                                await self.send(**res)

                            if not acl.allows("e"):
                                raise StopAsyncIteration
                            if not acl.allows("x"):
                                acl.block("r")

                        await entry.walk(worker, acl=acl, **kv)
                        await self.send(state="uptodate")

                    await tg.spawn(orig_state)

                async for m in watcher:
                    ml = len(m.entry.path) - len(msg.path)
                    if ml < min_depth:
                        continue
                    if max_depth >= 0 and ml > max_depth:
                        continue
                    a = acl
                    for p in getattr(m, "path", [])[shorter.depth :]:
                        if not a.allows("e"):
                            break
                        if not acl.allows("x"):
                            a.block("r")
                        a = a.step(p)
                    else:
                        res = m.entry.serialize(
                            chop_path=client._chop_path, nchain=nchain, conv=conv
                        )
                        shorter(res)
                        if not a.allows("r"):
                            res.pop("value", None)
                        await self.send(**res)


class SCmd_msg_monitor(StreamCommand):
    """
    Monitor a topic for changes.

    This is a pass-through command.
    """

    multiline = True

    async def run(self):
        msg = self.msg
        raw = msg.get("raw", False)
        topic = msg.topic
        if isinstance(topic, str):
            topic = P(topic)
        if len(topic) and topic[0][0] == ":":
            topic = P(self.client.server.cfg.root) + topic

        async with self.client.server.serf.monitor(*topic) as stream:
            async for resp in stream:
                if hasattr(resp, "topic"):
                    t = resp.topic
                    if isinstance(t, str):
                        t = t.split(".")
                else:
                    t = topic
                res = {"topic": t}
                if raw:
                    res["raw"] = resp.payload
                else:
                    try:
                        res["data"] = unpacker(resp.payload)
                    except Exception as exc:
                        res["raw"] = resp.payload
                        res["error"] = repr(exc)

                await self.send(**res)


class ServerClient:
    """Represent one (non-server) client."""

    is_chroot = False
    _user = None  # user during auth
    user = None  # authorized user
    _dh_key = None
    conv = ConvNull
    acl: ACLStepper = NullACL
    tg = None

    def __init__(self, server: "Server", stream: anyio.abc.Stream):
        self.server = server
        self.root = server.root
        self.metaroot = self.root.follow(Path(None), create=True, nulls_ok=True)
        self.stream = stream
        self.seq = 0
        self.tasks = {}
        self.in_stream = {}
        self._chop_path = 0
        self._send_lock = anyio.create_lock()

        global _client_nr
        _client_nr += 1
        self._client_nr = _client_nr
        self.logger = server.logger

    @property
    def nulls_ok(self):
        if self.is_chroot:
            return False
        if None not in self.root:
            return 2
        if self.user.is_super_root:
            return True
        # TODO test for superuser-ness, if so return True
        return False

    async def _process(self, fn, msg):
        res = await fn(msg)
        if res is None:
            res = {}
        elif not isinstance(res, dict):
            res = {"result": res}
        res["seq"] = msg.seq
        await self.send(res)

    async def process(self, msg, evt=None):
        """
        Process an incoming message.
        """
        needAuth = self.user is None or self._user is not None
        self.logger.debug("IN_%d %s", self._client_nr, msg)

        seq = msg.seq
        async with anyio.open_cancel_scope() as s:
            self.tasks[seq] = s
            if "chain" in msg:
                msg.chain = NodeEvent.deserialize(msg.chain, cache=self.server.node_cache)

            fn = None
            if msg.get("state", "") != "start":
                fn = getattr(self, "cmd_" + str(msg.action), None)
            if fn is None:
                fn = StreamCommand(self, msg)
                if needAuth and not getattr(fn, "noAuth", False):
                    raise NoAuthError()
            else:
                if needAuth and not getattr(fn, "noAuth", False):
                    raise NoAuthError()
                fn = partial(self._process, fn, msg)
            if evt is not None:
                await evt.set()

            try:
                await fn()

            except BrokenPipeError as exc:
                self.logger.info("ERR%d: %s", self._client_nr, repr(exc))

            except Exception as exc:
                if not isinstance(exc, ClientError):
                    self.logger.exception("ERR%d: %s", self._client_nr, repr(msg))
                await self.send({"error": str(exc), "seq": seq})

            finally:
                del self.tasks[seq]

    def _chroot(self, root):
        if not root:
            return
        entry, _acl = self.root.follow_acl(root, acl=self.acl, nulls_ok=False)

        self.root = entry
        self.is_chroot = True
        self._chop_path += len(root)

    async def cmd_diffie_hellman(self, msg):
        if self._dh_key:
            raise RuntimeError("Can't call dh twice")
        from diffiehellman.diffiehellman import DiffieHellman

        def gen_key():
            length = msg.get("length", 1024)
            k = DiffieHellman(key_length=length, group=(5 if length < 32 else 14))
            k.generate_public_key()
            k.generate_shared_secret(byte2num(msg.pubkey))
            self._dh_key = num2byte(k.shared_secret)[0:32]
            return k

        async with self.server.crypto_limiter:
            k = await anyio.run_in_thread(gen_key)
        return {"pubkey": num2byte(k.public_key)}

    cmd_diffie_hellman.noAuth = True

    @property
    def dh_key(self):
        if self._dh_key is None:
            raise RuntimeError("The client has not executed DH key exchange")
        return self._dh_key

    async def cmd_fake_info(self, msg):
        msg["node"] = ""
        msg["tick"] = 0
        self.logger.warning("Fake Info LOCAL %s", pformat(msg))
        await self.server.user_info(msg)

    async def cmd_fake_info_send(self, msg):
        msg["node"] = ""
        msg["tick"] = 0
        msg.pop("tock", None)
        self.logger.warning("Fake Info SEND %s", pformat(msg))
        await self.server._send_event("info", msg)

    async def cmd_auth_get(self, msg):
        class AuthGet(SingleMixin, SCmd_auth_get):
            pass

        return await AuthGet(self, msg)()

    async def cmd_auth_set(self, msg):
        class AuthSet(SingleMixin, SCmd_auth_set):
            pass

        return await AuthSet(self, msg)()

    async def cmd_auth_list(self, msg):
        class AuthList(SingleMixin, SCmd_auth_list):
            pass

        return await AuthList(self, msg)()

    async def cmd_auth_info(self, msg):
        msg["path"] = Path(None, "auth")
        return await self.cmd_get_internal(msg)

    async def cmd_root(self, msg):
        """Change to a sub-tree.
        """
        self._chroot(msg.path)
        return self.root.serialize(chop_path=self._chop_path, conv=self.conv)

    async def cmd_get_internal(self, msg):
        return await self.cmd_get_value(msg, root=self.metaroot, _nulls_ok=True)

    async def cmd_set_internal(self, msg):
        return await self.cmd_set_value(msg, root=self.metaroot, _nulls_ok=True)

    async def cmd_enum_internal(self, msg):
        return await self.cmd_enum(msg, root=self.metaroot, _nulls_ok=True)

    async def cmd_delete_internal(self, msg):
        return await self.cmd_delete_value(msg, root=self.metaroot)

    async def cmd_get_tock(self, msg):  # pylint: disable=unused-argument
        return {"tock": self.server.tock}

    async def cmd_test_acl(self, msg):
        """Check which ACL a path matches."""
        root = self.root
        mode = msg.get("mode") or "x"
        acl = self.acl
        acl2 = msg.get("acl", None)
        try:
            _entry, _acl = root.follow_acl(
                msg.path,
                acl=self.acl,
                acl_key="a" if acl2 is None else mode,
                nulls_ok=False,
                create=None,
            )

            if acl2 is not None:
                ok = acl.allows("a")  # pylint: disable=no-value-for-parameter # pylint is confused
                acl2 = root.follow(Path(None, "acl", acl2), create=False, nulls_ok=True)
                acl2 = ACLFinder(acl2)
                _entry, acl = root.follow_acl(
                    msg.path, acl=acl2, acl_key=mode, nulls_ok=False, create=None
                )
                if not ok:
                    acl.block("a")
                acl.check(mode)
        except ACLError:
            return {"access": False}
        else:
            return {"access": acl.result.data if acl.allows("a") else True}

    async def cmd_enum(self, msg, with_data=None, _nulls_ok=None, root=None):
        """Get all sub-nodes.
        """
        if root is None:
            root = self.root
        if with_data is None:
            with_data = msg.get("with_data", False)
        entry, acl = root.follow_acl(
            msg.path, acl=self.acl, acl_key="e", create=False, nulls_ok=_nulls_ok
        )
        empty = msg.get("empty", False)
        if with_data:
            res = {}
            for k, v in entry.items():
                a = acl.step(k)
                if a.allows("r"):
                    if v.data is not NotGiven and acl.allows("x"):
                        res[k] = self.conv.enc_value(v.data, entry=v)
                    elif empty:
                        res[k] = None
        else:
            res = []
            for k, v in entry.items():
                if empty or v.data is not NotGiven:
                    a = acl.step(k)
                    if a.allows("e"):
                        res.append(k)
        return {"result": res}

    cmd_enumerate = cmd_enum  # backwards compat: XXX remove

    async def cmd_enum_node(self, msg):
        n = msg.get("max", 0)
        cur = msg.get("current", False)
        node = Node(msg["node"], None, cache=self.server.node_cache, create=False)
        res = list(node.enumerate(n=n, current=cur))
        return {"result": res}

    async def cmd_kill_node(self, msg):
        node = msg["node"]
        node = Node(msg["node"], None, cache=self.server.node_cache, create=False)
        for k in node.enumerate(current=True):
            raise ServerError(f"Node {node.name} has entry {k}")

        await self.server.drop_node(node.name)

    async def cmd_get_value(self, msg, _nulls_ok=None, root=None):
        """Get a node's value.
        """
        if "node" in msg and "path" not in msg:
            n = Node(msg.node, cache=self.server.node_cache, create=False)
            return n[msg.tick].serialize(
                chop_path=self._chop_path, nchain=msg.get("nchain", 0), conv=self.conv
            )

        if _nulls_ok is None:
            _nulls_ok = self.nulls_ok
        if root is None:
            root = self.root
        try:
            entry, _ = root.follow_acl(
                msg.path, create=False, acl=self.acl, acl_key="r", nulls_ok=_nulls_ok
            )
        except KeyError:
            entry = {}
            if msg.get("nchain", 0):
                entry["chain"] = None
        else:
            entry = entry.serialize(chop_path=-1, nchain=msg.get("nchain", 0), conv=self.conv)
        return entry

    async def cmd_set_value(self, msg, **kw):
        """Set a node's value.
        """
        if "value" not in msg:
            raise ClientError("Call 'delete_value' if you want to clear the value")
        return await self._set_value(msg, value=msg.value, **kw)

    async def cmd_delete_value(self, msg, **kw):
        """Delete a node's value.
        """
        if "value" in msg:
            raise ClientError("A deleted entry can't have a value")

        return await self._set_value(msg, **kw)

    async def _set_value(self, msg, value=NotGiven, root=None, _nulls_ok=False):
        # TODO drop this as soon as we have server-side user mods
        if self.user.is_super_root and root is None:
            _nulls_ok = 2

        if root is None:
            root = self.root
            acl = self.acl
        else:
            acl = NullACL
        entry, acl = root.follow_acl(msg.path, acl=acl, acl_key="W", nulls_ok=_nulls_ok)
        if root is self.root and "match" in self.metaroot:
            try:
                self.metaroot["match"].check_value(None if value is NotGiven else value, entry)
            except ClientError:
                raise
            except Exception as exc:
                logger.exception("Err %s: %r", exc, msg)
                raise ClientError(repr(exc)) from None
                # TODO pass exceptions to the client

        send_prev = True
        nchain = msg.get("nchain", 1)

        if msg.get("idem", False) and type(entry.data) is type(value) and entry.data == value:
            res = attrdict(tock=entry.tock, changed=False)
            if nchain > 0:
                res.chain = entry.chain.serialize(nchain=nchain)
            return res

        if "prev" in msg:
            if entry.data != msg.prev:
                raise ClientError(f"Data is {entry.data !r} not {msg.prev !r} at {msg.path}")
            send_prev = False
        if "chain" in msg:
            if msg.chain is None:
                if entry.data is not NotGiven:
                    raise ClientChainError("Entry already exists at {msg.path}")
            elif entry.data is NotGiven:
                raise ClientChainError("Entry is new at {msg.path}")
            elif entry.chain != msg.chain:
                raise ClientChainError(
                    f"Chain is {entry.chain !r} not {msg.chain !r} for {msg.path}"
                )
            send_prev = False

        res = attrdict()
        if value is NotGiven:
            res.changed = entry.data is not NotGiven
        else:
            res.changed = entry.data != value
        if send_prev and entry.data is not NotGiven:
            res.prev = self.conv.enc_value(entry.data, entry=entry)

        nchain = msg.get("nchain", 1)
        value = msg.get("value", NotGiven)
        async with self.server.next_event() as event:
            await entry.set_data(
                event,
                NotGiven if value is NotGiven else self.conv.dec_value(value, entry=entry),
                server=self.server,
                tock=self.server.tock,
            )
        if nchain != 0:
            res.chain = entry.chain.serialize(nchain=nchain)
        res.tock = entry.tock

        return res

    async def cmd_update(self, msg):
        """
        Apply a stored update.

        You usually do this via a stream command.
        """
        msg = UpdateEvent.deserialize(
            self.root, msg, nulls_ok=self.nulls_ok, conv=self.conv, cache=self.server._nodes
        )
        res = await msg.entry.apply(msg, server=self, root=self.root)
        if res is None:
            return False
        else:
            return res.serialize(chop_path=self._chop_path, conv=self.conv)

    async def cmd_check_deleted(self, msg):
        nodes = msg.nodes
        deleted = NodeSet()
        for n, v in nodes.items():
            n = Node(n, None, cache=self.server.node_cache)
            r = RangeSet()
            r.__setstate__(v)
            for a, b in r:
                for t in range(a, b):
                    if t not in n:
                        deleted.add(n.name, t)
        if deleted:
            await self.server._send_event("info", attrdict(deleted=deleted.serialize()))

    async def cmd_get_state(self, msg):
        """Return some info about this node's internal state"""
        return await self.server.get_state(**msg)

    async def cmd_msg_send(self, msg):
        topic = msg.topic
        if isinstance(topic, str):
            topic = (topic,)
        if topic[0][0] == ":":
            topic = P(self.server.cfg.root) + topic
        if "raw" in msg:
            assert "data" not in msg
            data = msg.raw
        else:
            data = packer(msg.data)
        await self.server.serf.send(*topic, payload=data)

    async def cmd_delete_tree(self, msg):
        """Delete a node's value.
        Sub-nodes are cleared (after their parent).
        """
        seq = msg.seq
        if not msg.path:
            raise ClientError("You can't delete the root node")
        nchain = msg.get("nchain", 0)
        if nchain:
            await self.send({"seq": seq, "state": "start"})
        ps = PathShortener(msg.path)

        try:
            entry, acl = self.root.follow_acl(
                msg.path, acl=self.acl, acl_key="d", nulls_ok=self.nulls_ok
            )
        except KeyError:
            return False

        async def _del(entry, acl):
            res = 0
            if entry.data is not None and acl.allows("d"):
                async with self.server.next_event() as event:
                    evt = await entry.set_data(event, NotGiven, server=self, tock=self.server.tock)
                    if nchain:
                        r = evt.serialize(
                            chop_path=self._chop_path, nchain=nchain, with_old=True, conv=self.conv
                        )
                        r["seq"] = seq
                        r.pop("new_value", None)  # always None
                        ps(r)
                        await self.send(r)
                res += 1
            if not acl.allows("e") or not acl.allows("x"):
                return
            for v in entry.values():
                a = acl.step(v, new=True)
                res += await _del(v, a)
            return res

        res = await _del(entry, acl)
        if nchain:
            await self.send({"seq": seq, "state": "end"})
        else:
            return {"changed": res}

    async def cmd_log(self, msg):
        await self.server.run_saver(path=msg.path, save_state=msg.get("fetch", False))
        return True

    async def cmd_save(self, msg):
        full = msg.get("full", False)
        await self.server.save(path=msg.path, full=full)

        return True

    async def cmd_stop(self, msg):
        try:
            t = self.tasks[msg.task]
        except KeyError:
            return False
        await t.cancel()
        return True

    async def cmd_set_auth_typ(self, msg):
        if not self.user.is_super_root:
            raise RuntimeError("You're not allowed to do that")
        a = self.root.follow(Path(None, "auth"), nulls_ok=True)
        if a.data is NotGiven:
            val = {}
        else:
            val = a.data.copy()

        if msg.typ is None:
            val.pop("current", None)
        elif msg.typ not in a or not len(a[msg.typ]["user"].keys()):
            raise RuntimeError("You didn't configure this method yet:" + repr((msg.typ, vars(a))))
        else:
            val["current"] = msg.typ
        msg.value = val
        msg.path = (None, "auth")
        return await self.cmd_set_value(msg, _nulls_ok=True)

    async def send(self, msg):
        self.logger.debug("OUT%d %s", self._client_nr, msg)
        if self._send_lock is None:
            return
        async with self._send_lock:
            if self._send_lock is None:
                # yes this can happen, when the connection is torn down
                return

            if "tock" not in msg:
                msg["tock"] = self.server.tock
            try:
                await self.stream.send_all(packer(msg))
            except (anyio.exceptions.ClosedResourceError, trioBrokenResourceError):
                self.logger.info("ERO%d %r", self._client_nr, msg)
                self._send_lock = None
                raise

    async def send_result(self, seq, res):
        res["seq"] = seq
        if "tock" in res:
            await self.server.tock_seen(res["tock"])
        else:
            res["tock"] = self.server.tock
        await self.send(res)

    async def run(self):
        """Main loop for this client connection."""
        unpacker_ = stream_unpacker()  # pylint: disable=redefined-outer-name

        async with anyio.create_task_group() as tg:
            self.tg = tg
            msg = {
                "seq": 0,
                "version": _version_tuple,
                "node": self.server.node.name,
                "tick": self.server.node.tick,
                "tock": self.server.tock,
            }
            try:
                auth = self.root.follow(Path(None, "auth"), nulls_ok=True, create=False)
            except KeyError:
                a = None
            else:
                auths = list(auth.keys())
                try:
                    a = auth.data["current"]
                except (ValueError, KeyError, IndexError, TypeError):
                    a = None
                else:
                    try:
                        auths.remove(a)
                    except ValueError:
                        a = None
                auths.insert(0, a)
                msg["auth"] = auths
            if a is None:
                from .auth import RootServerUser

                self.user = RootServerUser()
            await self.send(msg)

            while True:
                for msg in unpacker_:
                    seq = None
                    try:
                        seq = msg.seq
                        send_q = self.in_stream.get(seq, None)
                        if send_q is not None:
                            await send_q.received(msg)
                        else:
                            if self.seq >= seq:
                                raise ClientError(
                                    f"Channel closed? Sequence error: {self.seq} < {msg.seq}"
                                )
                            self.seq = seq
                            evt = anyio.create_event()
                            await self.tg.spawn(self.process, msg, evt)
                            await evt.wait()
                    except Exception as exc:
                        msg = {"error": str(exc)}
                        if isinstance(exc, ClientError):  # pylint doesn't seem to see this, so …:
                            msg["etype"] = exc.etype  # pylint: disable=no-member  ### YES IT HAS
                        else:
                            self.logger.exception(
                                "ERR %d: Client error on %s", self._client_nr, repr(msg)
                            )
                        msg = {"error": str(exc)}
                        if seq is not None:
                            msg["seq"] = seq
                        await self.send(msg)

                try:
                    buf = await self.stream.receive_some(4096)
                except (ConnectionResetError, trioBrokenResourceError):
                    self.logger.info("DEAD %d", self._client_nr)
                    break
                if len(buf) == 0:  # Connection was closed.
                    self.logger.debug("CLOSED %d", self._client_nr)
                    break
                unpacker_.feed(buf)

            await tg.cancel_scope.cancel()

    def drop_old_event(self, evt, old_evt=NotGiven):
        return self.server.drop_old_event(evt, old_evt)

    def mark_deleted(self, node, tick):
        return self.server.mark_deleted(node, tick)


class _RecoverControl:
    _id = 0

    def __init__(self, server, scope, prio, local_history, sources):
        self.server = server
        self.scope = scope
        self.prio = prio

        local_history = set(local_history)
        sources = set(sources)
        self.local_history = local_history - sources
        self.sources = sources - local_history
        self.tock = server.tock
        type(self)._id += 1
        self._id = type(self)._id

        self._waiters = {}

    async def _start(self):
        chk = set()
        rt = self.server._recover_tasks
        for node in self.local_history:
            xrc = rt.get(node, None)
            if xrc is not None:
                chk.add(xrc)
            self.server._recover_tasks[node] = self
        for t in chk:
            await t._check()

    async def _check(self):
        lh = []
        rt = self.server._recover_tasks
        for n in self.local_history:
            if rt.get(n, None) is self:
                lh.append(n)
            self.local_history = lh
            if not lh:
                await self.cancel()

    def __hash__(self):
        return id(self)

    async def cancel(self):
        await self.scope.cancel()
        rt = self.server._recover_tasks
        for node in self.local_history:
            if rt.get(node, None) is self:
                del rt[node]
        self.local_history = ()
        for evt in list(self._waiters.values()):
            await evt.set()

    async def set(self, n):
        evt = self._waiters.get(n, None)
        if evt is None:
            evt = anyio.create_event()
            self._waiters[n] = evt
        await evt.set()

    async def wait(self, n):
        evt = self._waiters.get(n, None)
        if evt is None:
            evt = anyio.create_event()
            self._waiters[n] = evt
        await evt.wait()


class Server:
    """
    This is the DistKV server. It manages connections to the Serf/MQTT server,
    the DistKV clients, and (optionally) logs all changes to a file.

    Args:
      name (str): the name of this DistKV server instance.
        It **must** be unique.
      cfg: configuration.
        See :attr:`distkv.default.CFG` for default values.
        Relevant is the ``server`` sub-dict (mostly).
      init (Any):
        The initial content of the root entry. **Do not use this**, except
          when setting up an entirely new DistKV network.
    """

    # pylint: disable=no-member # mis-categorizing cfg as tuple
    serf = None
    _ready = None
    _ready2 = None
    _actor = None
    _del_actor = None
    cfg: attrdict = None
    force_startup: bool = False

    seen_missing = None
    fetch_running = None
    sending_missing = None
    ports = None
    _tock = 0

    def __init__(self, name: str, cfg: dict = None, init: Any = NotGiven):
        self.root = RootEntry(self, tock=self.tock)

        self.cfg = combine_dict(cfg or {}, CFG, cls=attrdict)
        if isinstance(self.cfg.server.root, str):
            self.cfg.server.root = P(self.cfg.server.root)
        else:
            self.cfg.server.root = Path.build(self.cfg.server.root)

        self.paranoid_root = self.root if self.cfg.server.paranoia else None

        self._nodes: Dict[str, Node] = {}
        self.node_drop = set()
        self.node = Node(name, None, cache=self.node_cache)

        self._init = init
        self.crypto_limiter = anyio.create_semaphore(3)
        self.logger = logging.getLogger("distkv.server." + name)
        self._delete_also_nodes = NodeSet()

        # Lock for generating a new node event
        self._evt_lock = anyio.create_lock()

        # connected clients
        self._clients = set()

        # cache for partial messages
        self._part_len = SERF_MAXLEN - SERF_LEN_DELTA - len(self.node.name)
        self._part_seq = 0
        self._part_cache = dict()

        self._savers = []

        # This is here, not in _run_del, because _del_actor needs to be accessible early
        self._del_actor = DeleteActor(self)

    @property
    def node_cache(self):
        """
        A node cache helper which also removes new nodes from the node_drop set.
        """

        class Cache:
            def __len__(slf):  # pylint: disable=no-self-argument
                return len(self._nodes)

            def __bool__(slf):  # pylint: disable=no-self-argument
                return len(self._nodes) > 0

            def __contains__(slf, k):  # pylint: disable=no-self-argument
                return k in self._nodes

            def __getitem__(slf, k):  # pylint: disable=no-self-argument
                return self._nodes[k]

            def __setitem__(slf, k, v):  # pylint: disable=no-self-argument
                self._nodes[k] = v
                self.node_drop.discard(k)

            def __delitem__(slf, k):  # pylint: disable=no-self-argument
                del self._nodes[k]
                self.node_drop.add(k)

            def get(slf, *k):  # pylint: disable=no-self-argument
                return self._nodes.get(*k)

            def pop(slf, *k):  # pylint: disable=no-self-argument
                self.node_drop.add(k)
                return self._nodes.pop(*k)

        return Cache()

    @asynccontextmanager
    async def next_event(self):
        """A context manager which returns the next event under a lock.

        This increments ``tock`` because that increases the chance that the
        node (or split) where something actually happens wins a collision.

        Rationale: if the event is created and leaks to the environment, it
        needs to be marked as deleted if incomplete. Otherwise the system
        sees it as "lost" data.
        """
        async with self._evt_lock:
            n = None
            try:
                self.node.tick += 1
                nt = self.node.tick
                self._tock += 1
                await self._set_tock()  # updates actor
                n = NodeEvent(self.node)
                yield n
            except BaseException as exc:
                if n is not None:
                    self.logger.warning("Deletion %s %d due to %r", self.node, n.tick, exc)
                    self.node.report_deleted(RangeSet((nt,)), self)
                    async with anyio.move_on_after(2, shield=True):
                        await self._send_event(
                            "info", dict(node="", tick=0, deleted={self.node.name: (nt,)})
                        )
                raise
            finally:
                self._tock += 1
                # does not update actor again, once is sufficient

    @property
    def tock(self):
        """Retrieve ``tock``.

        Also increments it because tock values may not be re-used."""
        self._tock += 1
        return self._tock

    async def tock_seen(self, value):
        """
        Updates the current tock value so that it is at least ``value``.

        Args:
          value (int): some incoming '`tock``.
        """
        if value is None:
            return
        if self._tock < value:
            self._tock = value
            await self._set_tock()

    async def _set_tock(self):
        if self._actor is not None and self._ready.is_set():
            await self._actor.set_value((self._tock, self.node.tick))

    async def del_check(self, value):
        """
        Called when ``(None,"actor","del")`` is set.
        """
        if value is NotGiven:
            await self._del_actor.disable()
            return

        nodes = value.get("nodes", ())
        if self.node.name in nodes:
            await self._del_actor.enable(len(nodes))
        else:
            await self._del_actor.disable(len(nodes))

    def drop_old_event(self, evt, old_evt=NotGiven):
        """
        Drop either one event, or any event that is in ``old_evt`` but not
        in ``evt``.
        """
        if old_evt is None:
            return
        if old_evt is NotGiven:
            evt.node.supersede(evt.tick)
            return

        nt = {}
        while evt is not None:
            assert evt.node.name not in nt
            nt[evt.node.name] = evt.tick
            evt = evt.prev
        while old_evt is not None:
            if nt.get(old_evt.node.name, 0) != old_evt.tick:
                old_evt.node.supersede(old_evt.tick)
            old_evt = old_evt.prev

    async def _send_event(self, action: str, msg: dict):
        """
        Helper to send a Serf event to the ``action`` endpoint.

        Args:
          action (str): the endpoint to send to. Prefixed by ``cfg.root``.
          msg: the message to send.
        """
        if "tock" not in msg:
            msg["tock"] = self.tock
        else:
            await self.tock_seen(msg["tock"])
        if "node" not in msg:
            msg["node"] = self.node.name
        if "tick" not in msg:
            msg["tick"] = self.node.tick
        self.logger.debug("Send %s: %r", action, msg)
        for m in self._pack_multiple(msg):
            await self.serf.send(*self.cfg.server.root, action, payload=m)

    async def watcher(self):
        """
        The background task that watches a (sub)tree for changes.
        """
        async with Watcher(self.root, q_len=0, full=True) as watch:
            async for msg in watch:
                self.logger.debug("Watch: %r", msg)
                if msg.event.node != self.node:
                    continue
                if self.node.tick is None:
                    continue
                p = msg.serialize(nchain=self.cfg.server.change.length)
                await self._send_event("update", p)

    async def resync_deleted(self, nodes):
        """
        Owch. We need to re-sync.

        We collect the latest ticks in our object tree and send them to one
        of the Delete nodes.
        """

        for n in nodes:
            try:
                host, port = await self._get_host_port(n)
                cfg = combine_dict(
                    {"host": host, "port": port, "name": self.node.name},
                    self.cfg.connect,
                    cls=attrdict,
                )
                auth = cfg.get("auth", None)
                from .auth import gen_auth

                cfg["auth"] = gen_auth(auth)

                self.logger.debug("DelSync: connecting %s", cfg)
                async with distkv_client.open_client(connect=cfg) as client:
                    # TODO auth this client
                    nodes = NodeSet()
                    n_nodes = 0

                    async def send_nodes():
                        nonlocal nodes, n_nodes
                        await client._request(  # pylint: disable=cell-var-from-loop
                            "check_deleted", iter=False, nchain=-1, nodes=nodes.serialize()
                        )
                        nodes.clear()
                        n_nodes = 0

                    async def add(event):
                        nonlocal nodes, n_nodes
                        c = event.chain
                        if c is None:
                            return
                        nodes.add(c.node.name, c.tick)
                        n_nodes += 1
                        if n_nodes >= 100:
                            await send_nodes()

                    await self.root.walk(add)
                    if n_nodes > 0:
                        await send_nodes()

            except (ServerConnectionError, ServerClosedError):
                self.logger.exception("Unable to connect to %s", nodes)
            else:
                # The recipient will broadcast "info.deleted" messages for
                # whatever it doesn't have, so we're done here.
                return

    def mark_deleted(self, node, tick):
        """
        This tick has been marked as deleted.
        """
        self._delete_also_nodes[node.name].add(tick)

    def purge_deleted(self, deleted):
        """
        These deleted entry is no longer required.
        """
        self.logger.debug("PurgeDel: %r", deleted)

        for n, v in deleted.items():
            n = Node(n, cache=self.node_cache)
            n.purge_deleted(v)

    async def get_state(
        self,
        nodes=False,
        known=False,
        superseded=False,
        deleted=False,
        missing=False,
        present=False,
        node_drop=False,
        debug=False,
        debugger=False,
        remote_missing=False,
        **_kw,
    ):
        """
        Return some info about this node's internal state.
        """
        if known:
            superseded = True

        res = attrdict()
        if nodes:
            nd = res.nodes = {}
            for n in self._nodes.values():
                nd[n.name] = n.tick
        if superseded:
            nd = res.known = {}
            for n in self._nodes.values():
                lk = n.local_superseded
                if len(lk):
                    nd[n.name] = lk.__getstate__()
        if present:
            nd = res.present = {}
            for n in self._nodes.values():
                lk = n.local_present
                if len(lk):
                    nd[n.name] = lk.__getstate__()
        if deleted:
            nd = res.deleted = {}
            for n in self._nodes.values():
                lk = n.local_deleted
                if len(lk):
                    nd[n.name] = lk.__getstate__()
        if missing:
            nd = res.missing = {}
            for n in self._nodes.values():
                if not n.tick:
                    continue
                lk = n.local_missing
                if len(lk):
                    nd[n.name] = lk.__getstate__()
        if remote_missing:
            nd = res.remote_missing = {}
            for n in self._nodes.values():
                lk = n.remote_missing
                if len(lk):
                    nd[n.name] = lk.__getstate__()
        if node_drop:
            res.node_drop = list(self.node_drop)
        if debug:
            nd = res.debug = attrdict()
            # TODO insert some debugging info

        if debugger:
            try:
                import pdb_clone as pdb
            except ImportError:
                res["debugger"] = "Import error"
            else:
                pdb().set_trace_remote(host=b"127.0.0.1", port=57935)

        res["node"] = self.node.name
        res["tock"] = self.tock
        return res

    async def user_update(self, msg):
        """
        Process an update message: deserialize it and apply the result.
        """
        msg = UpdateEvent.deserialize(self.root, msg, cache=self.node_cache, nulls_ok=True)
        await msg.entry.apply(msg, server=self, root=self.paranoid_root)

    async def user_info(self, msg):
        """
        Process info broadcasts.
        """

        if msg.node == self.node.name:
            return  # ignore our own message

        # Step 1
        ticks = msg.get("ticks", None)
        if ticks is not None:
            for n, t in ticks.items():
                n = Node(n, cache=self.node_cache)
                n.tick = max_n(n.tick, t)

            # did this message pre-empt our own transmission?
            rec = self._recover_tasks.get(msg.node, None)
            if rec is not None:
                await rec.set(1)
                self.logger.debug("Step1: %r triggered by %s", rec, msg.node)

        # Step 2
        missing = msg.get("missing", None)
        if missing is not None:
            nn = 0
            for n, k in missing.items():
                n = Node(n, cache=self.node_cache)
                r = RangeSet()
                r.__setstate__(k)
                nn += len(r)
                n.report_missing(r)

                # add to the node's seen_missing
                mr = self.seen_missing.get(n, None)
                if mr is None:
                    self.seen_missing[n] = r
                else:
                    mr += r

            # did this message pre-empt our own transmission?
            rec = self._recover_tasks.get(msg.node, None)
            if rec is not None:
                await rec.set(2)
                self.logger.debug("Step2: %r triggered by %s", rec, msg.node)

            if nn > 0:
                # Some data have been reported to be missing.
                # Send them.
                self.logger.debug("MISS %d %r", nn, self.seen_missing)
                await self._run_send_missing(None)

        # Step 3
        superseded = msg.get("superseded", None)
        if superseded is None:
            superseded = msg.get("known", None)
        if superseded is not None:
            for n, k in superseded.items():
                n = Node(n, cache=self.node_cache)
                r = RangeSet()
                r.__setstate__(k)
                r -= n.local_present
                # might happen when loading stale data
                n.report_superseded(r)

        deleted = msg.get("deleted", None)
        if deleted is not None:
            for n, k in deleted.items():
                n = Node(n, cache=self.node_cache)
                r = RangeSet()
                r.__setstate__(k)
                n.report_deleted(r, self)

        # Dropped nodes.
        for nn in msg.get("node_drop", ()):
            self._dropped_node(nn)

    async def _delete_also(self):
        """
        Add deletion records to the delete actor.
        """
        while True:
            await anyio.sleep(10)
            if self._delete_also_nodes:
                self._del_actor.add_deleted(self._delete_also_nodes)
                self._delete_also_nodes = NodeSet()

    def _pack_multiple(self, msg):
        """
        """
        # protect against mistakenly encoded multi-part messages
        # TODO use a msgpack extension instead
        if isinstance(msg, Mapping):
            i = 0
            while (f"_p{i}") in msg:
                i += 1
            j = i
            while i:
                i -= 1
                msg[f"_p{i+1}"] = msg[f"_p{i}"]
            if j:
                msg["_p0"] = ""

        p = packer(msg)
        pl = self._part_len
        if len(p) > SERF_MAXLEN:
            # Owch. We need to split this thing.
            self._part_seq = seq = self._part_seq + 1
            i = 0
            while i >= 0:
                i += 1
                px, p = p[:pl], p[pl:]
                if not p:
                    i = -i
                px = {"_p0": (self.node.name, seq, i, px)}
                yield packer(px)
            return
        yield p

    def _unpack_multiple(self, msg):
        """
        Undo the effects of _pack_multiple.
        """

        if isinstance(msg, Mapping) and "_p0" in msg:
            p = msg["_p0"]
            if p != "":
                nn, seq, i, p = p
                s = self._part_cache.get((nn, seq), None)
                if s is None:
                    self._part_cache[(nn, seq)] = s = [None]
                if i < 0:
                    i = -i
                    s[0] = b""
                while len(s) <= i:
                    s.append(None)
                s[i] = p
                if None in s:
                    return None
                p = b"".join(s)
                del self._part_cache[(nn, seq)]
                msg = unpacker(p)
                msg["_p0"] = ""

            i = 0
            while f"_p{i+1}" in msg:
                msg[f"_p{i}"] = msg[f"_p{i+1}"]
                i += 1
            del msg[f"_p{i}"]
        return msg

    async def monitor(self, action: str, delay: anyio.abc.Event = None):
        """
        The task that hooks to Serf's event stream for receiving messages.

        Args:
          action: The action name, corresponding to a Serf ``user_*`` method.
          delay: an optional event to wait for, after starting the
            listener but before actually processing messages. This helps to
            avoid consistency problems on startup.
        """
        cmd = getattr(self, "user_" + action)
        try:
            async with self.serf.monitor(*self.cfg.server.root, action) as stream:
                if delay is not None:
                    await delay.wait()

                async for resp in stream:
                    msg = unpacker(resp.payload)
                    msg = self._unpack_multiple(msg)
                    if not msg:  # None, empty, whatever
                        continue
                    self.logger.debug("Recv %s: %r", action, msg)
                    async with anyio.fail_after(10):
                        await self.tock_seen(msg.get("tock", 0))
                        await cmd(msg)
        except (CancelledError, trioCancelled):
            self.logger.warning("Cancelled %s", action)
            raise
        except BaseException as exc:
            self.logger.exception("Died %s: %r", action, exc)
            raise
        else:
            self.logger.error("Stream ended %s", action)

    async def _run_del(self, evt):
        try:
            await self._del_actor.run(evt=evt)
        finally:
            self._del_actor = None

    async def _pinger(self, delay: anyio.abc.Event):
        """
        This task
        * sends PING messages
        * handles incoming pings
        * triggers split recovery

        The initial ping is delayed randomly.

        Args:
          delay: an event to set after the initial ping message has been
            sent.
        """
        T = get_transport("distkv")
        async with Actor(
            T(self.serf, *self.cfg.server.root, "ping"),
            name=self.node.name,
            cfg=self.cfg.server.ping,
            send_raw=True,
        ) as actor:
            self._actor = actor
            await self._check_ticked()
            await delay.set()
            async for msg in actor:
                # self.logger.debug("IN %r",msg)

                if isinstance(msg, RecoverEvent):
                    await self.spawn(
                        self.recover_split,
                        msg.prio,
                        msg.replace,
                        msg.local_nodes,
                        msg.remote_nodes,
                    )

                elif isinstance(msg, GoodNodeEvent):
                    await self.spawn(self.fetch_data, msg.nodes)

                elif isinstance(msg, RawMsgEvent):
                    msg = msg.msg
                    msg_node = msg.get("node", None)
                    if msg_node is None:
                        msg_node = msg.get("history", (None,))[0]
                        if msg_node is None:
                            continue
                    val = msg.get("value", None)
                    tock = None
                    if val is not None:
                        tock, val = val
                        await self.tock_seen(tock)
                    node = Node(msg_node, val, cache=self.node_cache)
                    if tock is not None:
                        node.tock = tock

                elif isinstance(msg, TagEvent):
                    # We're "it"; find missing data
                    await self._send_missing()

                elif isinstance(msg, (TagEvent, UntagEvent, DetagEvent)):
                    pass
                    # TODO tell clients, for cleanup tasks in handlers,
                    # e.g. error needs to consolidate messages

    async def _get_host_port(self, host):
        """Retrieve the remote system to connect to.

        WARNING: While this is nice, there'a chicken-and-egg problem here.
        While you can use the hostmap to temporarily add new hosts with
        unusual addresses, the new host still needs a config entry.
        """
        port = self.cfg.connect.port
        domain = self.cfg.domain
        try:
            # First try to read the host name from the meta-root's
            # "hostmap" entry, if any.
            hme = self.root.follow(Path(None, "hostmap", host), create=False, nulls_ok=True)
            if hme.data is NotGiven:
                raise KeyError(host)
        except KeyError:
            hostmap = self.cfg.hostmap
            if host in hostmap:
                host = hostmap[host]
                if not isinstance(host, str):
                    # must be a 2-element tuple
                    host, port = host
                else:
                    # If it's a string, the port may have been passed as
                    # part of the hostname. (Notably on the command line.)
                    try:
                        host, port = host.rsplit(":", 1)
                    except ValueError:
                        pass
                    else:
                        port = int(port)
        else:
            # The hostmap entry in the database must be a tuple
            host, port = hme.data

        if domain is not None and "." not in host and host != "localhost":
            host += "." + domain
        return (host, port)

    async def do_send_missing(self):
        """Task to periodically send "missing …" messages
        """
        self.logger.debug("send-missing started")
        clock = self.cfg.server.ping.gap
        while self.fetch_missing:
            if self.fetch_running is not False:
                self.logger.debug("send-missing halted")
                return
            clock *= self._actor.random / 2 + 1
            await anyio.sleep(clock)

            n = 0
            msg = dict()
            for n in list(self.fetch_missing):
                m = n.local_missing
                nl = len(m)
                if nl == 0:
                    self.fetch_missing.remove(n)
                    continue

                mr = self.seen_missing.get(n.name, None)
                if mr is not None:
                    m -= mr
                if len(m) == 0:
                    continue
                msg[n.name] = m.__getstate__()
            self.seen_missing = {}
            if not n:  # nothing more to do
                break
            if not len(msg):  # others already did the work, this time
                continue
            msg = attrdict(missing=msg)
            self.logger.warning("Missing data: %r", msg)
            await self._send_event("info", msg)

        self.logger.debug("send-missing ended")
        if self.node.tick is None:
            self.node.tick = 0
            await self._check_ticked()
        self.fetch_running = None

    async def fetch_data(self, nodes, authoritative=False):
        """
        We are newly started and don't have any data.

        Try to get the initial data from some other node.
        """
        if self.fetch_running is not None:
            return
        self.fetch_running = True
        for n in nodes:
            try:
                host, port = await self._get_host_port(n)
                cfg = combine_dict(
                    {"host": host, "port": port, "name": self.node.name},
                    self.cfg.connect,
                    cls=attrdict,
                )
                auth = cfg.get("auth", None)
                from .auth import gen_auth

                cfg["auth"] = gen_auth(auth)

                async with distkv_client.open_client(connect=cfg) as client:
                    # TODO auth this client

                    pl = PathLongener(())
                    res = await client._request(
                        "get_tree", iter=True, from_server=self.node.name, nchain=-1, path=()
                    )
                    async for r in res:
                        pl(r)
                        r = UpdateEvent.deserialize(
                            self.root, r, cache=self.node_cache, nulls_ok=True
                        )
                        await r.entry.apply(r, server=self, root=self.paranoid_root)
                    await self.tock_seen(res.end_msg.tock)

                    pl = PathLongener((None,))
                    res = await client._request(
                        "get_tree_internal",
                        iter=True,
                        from_server=self.node.name,
                        nchain=-1,
                        path=(),
                    )
                    async for r in res:
                        pl(r)
                        r = UpdateEvent.deserialize(
                            self.root, r, cache=self.node_cache, nulls_ok=True
                        )
                        await r.entry.apply(r, server=self, root=self.paranoid_root)
                    await self.tock_seen(res.end_msg.tock)

                    res = await client._request(
                        "get_state",
                        nodes=True,
                        from_server=self.node.name,
                        known=True,
                        deleted=True,
                        iter=False,
                    )
                    await self._process_info(res)

            except (AttributeError, KeyError, ValueError, AssertionError, TypeError):
                raise
            except Exception:
                self.logger.exception("Unable to connect to %s:%d", host, port)
            else:
                # At this point we successfully cloned some other
                # node's state, so we now need to find whatever that
                # node didn't have.

                if authoritative:
                    # … or not.
                    self._discard_all_missing()
                for nst in self._nodes.values():
                    if nst.tick and len(nst.local_missing):
                        self.fetch_missing.add(nst)
                if len(self.fetch_missing):
                    self.fetch_running = False
                    await self.spawn(self.do_send_missing)
                if self.force_startup or not len(self.fetch_missing):
                    if self.node.tick is None:
                        self.node.tick = 0
                    self.fetch_running = None
                    await self._check_ticked()
                return

        self.fetch_running = None

    async def _process_info(self, msg):
        """
        Process "info" messages.
        """
        await self.tock_seen(msg.get("tock", 0))

        # nodes: list of known nodes and their max ticks
        for nn, t in msg.get("nodes", {}).items():
            nn = Node(nn, cache=self.node_cache)
            nn.tick = max_n(nn.tick, t)

        # known: per-node range of ticks that have been resolved
        for nn, k in msg.get("known", {}).items():
            nn = Node(nn, cache=self.node_cache)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_superseded(r, local=True)

        # deleted: per-node range of ticks that have been deleted
        deleted = msg.get("deleted", {})
        for nn, k in deleted.items():
            nn = Node(nn, cache=self.node_cache)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_deleted(r, self)

        # remote_missing: per-node range of ticks that should be re-sent
        # This is used when loading data from a state file
        for nn, k in msg.get("remote_missing", {}).items():
            nn = Node(nn, cache=self.node_cache)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_missing(r)

        # Dropped nodes.
        for nn in msg.get("node_drop", ()):
            self._dropped_node(nn)

    async def drop_node(self, name):
        self._dropped_node(name)
        await self._send_event("info", attrdict(node_drop=[name]))

    def _dropped_node(self, name):
        try:
            nn = Node(name, cache=self.node_cache, create=False)
        except KeyError:
            return
        for _ in nn.enumerate(current=True):
            break
        else:  # no item found
            nn.kill_this_node(self.node_cache)

    async def _check_ticked(self):
        if self._ready is None:
            return
        if self.node.tick is not None:
            self.logger.debug("Ready")
            await self._ready.set()
            await self._set_tock()
        else:
            # self.logger.debug("Not yet ready.")
            pass

    async def recover_split(self, prio, replace, local_history, sources):
        """
        Recover from a network split.
        """
        async with anyio.open_cancel_scope() as scope:
            for node in sources:
                if node not in self._recover_tasks:
                    break
            else:
                return
            t = _RecoverControl(self, scope, prio, local_history, sources)
            self.logger.debug(
                "SplitRecover %d: start %d %s local=%r remote=%r",
                t._id,
                prio,
                replace,
                local_history,
                sources,
            )
            try:
                await t._start()
                clock = self.cfg.server.ping.cycle

                # Step 1: send an info/ticks message
                # for prio=0 this fires immediately. That's intentional.
                async with anyio.move_on_after(clock * (1 - 1 / (1 << prio))) as x:
                    await t.wait(1)
                if x.cancel_called:
                    msg = dict((x.name, x.tick) for x in self._nodes.values())

                    msg = attrdict(ticks=msg)
                    if self.node_drop:
                        msg.node_drop = list(self.node_drop)
                    await self._send_event("info", msg)

                # Step 2: send an info/missing message
                # for prio=0 this fires after clock/2, so that we get a
                # chance to wait for other info/ticks messages. We can't
                # trigger on them because there may be more than one, for a
                # n-way merge.
                async with anyio.move_on_after(clock * (2 - 1 / (1 << prio)) / 2) as x:
                    await t.wait(2)

                if x.cancel_called:
                    await self._send_missing(force=True)

                # wait a bit more before continuing. Again this depends on
                # `prio` so that there won't be two nodes that send the same
                # data at the same time, hopefully.
                await anyio.sleep(clock * (1 - 1 / (1 << prio)))

                # Step 3: start a task that sends stuff
                await self._run_send_missing(prio)

            finally:
                async with anyio.open_cancel_scope(shield=True):
                    # Protect against cleaning up when another recovery task has
                    # been started (because we saw another merge)
                    self.logger.debug("SplitRecover %d: finished @%d", t._id, t.tock)
                    self.seen_missing = {}
                    await t.cancel()

    async def _send_missing(self, force=False):
        msg = dict()
        for n in list(self._nodes.values()):
            if not n.tick:
                continue
            m = n.local_missing
            mr = self.seen_missing.get(n.name, None)
            if mr is not None:
                m -= mr
            if len(m) == 0:
                continue
            msg[n.name] = m.__getstate__()
            if mr is None:
                self.seen_missing[n.name] = m
            else:
                mr += m

        if force or msg:
            msg = attrdict(missing=msg)
            if self.node_drop:
                msg.node_drop = list(self.node_drop)
            await self._send_event("info", msg)

    async def _run_send_missing(self, prio):
        """Start :meth:`_send_missing_data` if it's not running"""

        if self.sending_missing is None:
            self.sending_missing = True
            await self.spawn(self._send_missing_data, prio)
        elif not self.sending_missing:
            self.sending_missing = True

    async def _send_missing_data(self, prio):
        """Step 3 of the re-join protocol.
        For each node, collect events that somebody has reported as missing,
        and re-broadcast them. If the event is unavailable, send a "known"
        / "deleted" message.
        """

        self.logger.debug("SendMissing %s", prio)
        clock = self.cfg.server.ping.cycle
        if prio is None:
            await anyio.sleep(clock * (1 + self._actor.random / 3))
        else:
            await anyio.sleep(clock * (1 - (1 / (1 << prio)) / 2 - self._actor.random / 5))

        self.logger.debug("SendMissingGo %s %s", prio, self.sending_missing)
        while self.sending_missing:
            self.sending_missing = False
            nodes = list(self._nodes.values())
            self._actor._rand.shuffle(nodes)
            known = {}
            deleted = {}
            for n in nodes:
                self.logger.debug(
                    "SendMissingGo %s %r %r", n.name, n.remote_missing, n.local_superseded
                )
                k = n.remote_missing & n.local_superseded
                for r in n.remote_missing & n.local_present:
                    for t in range(*r):
                        if t not in n.remote_missing:
                            # some other node could have sent this while we worked
                            await anyio.sleep(self.cfg.server.ping.gap / 3)
                            continue
                        if t in n:
                            # could have been deleted while sleeping
                            msg = n[t].serialize()
                            await self._send_event("update", msg)
                            n.remote_missing.discard(t)
                if k:
                    known[n.name] = k.__getstate__()

                d = n.remote_missing & n.local_deleted
                if d:
                    deleted[n.name] = d.__getstate__()

            msg = attrdict()
            if known:
                msg.known = known
            if deleted:
                msg.deleted = deleted
            if self.node_drop:
                msg.node_drop = list(self.node_drop)
            if msg:
                await self._send_event("info", attrdict(known=known, deleted=deleted))
        self.sending_missing = None

    async def load(
        self,
        path: str = None,
        stream: io.IOBase = None,
        local: bool = False,
        authoritative: bool = False,
    ):
        """Load data from this stream

        Args:
          ``fd``: The stream to read.
          ``local``: Flag whether this file contains initial data and thus
                     its contents shall not be broadcast. Don't set this if
                     the server is already operational.
        """
        longer = PathLongener(())

        if local and self.node.tick is not None:
            raise RuntimeError("This server already has data.")
        elif not local and self.node.tick is None:
            raise RuntimeError("This server is not yet operational.")
        async with MsgReader(path=path, stream=stream) as rdr:
            async for m in rdr:
                if "value" in m:
                    longer(m)
                    if "tock" in m:
                        await self.tock_seen(m.tock)
                    else:
                        m.tock = self.tock
                    m = UpdateEvent.deserialize(self.root, m, cache=self.node_cache, nulls_ok=True)
                    await self.tock_seen(m.tock)
                    await m.entry.apply(m, server=self, root=self.paranoid_root, loading=True)
                elif "info" in m:
                    await self._process_info(m["info"])
                elif "nodes" in m or "known" in m or "deleted" in m or "tock" in m:  # XXX LEGACY
                    await self._process_info(m)
                else:
                    self.logger.warning("Unknown message in stream: %s", repr(m))

        if authoritative:
            self._discard_all_missing()

        self.logger.debug("Loading finished.")

    def _discard_all_missing(self):
        for n in self._nodes.values():
            if not n.tick:
                continue
            lk = n.local_missing

            if len(lk):
                n.report_superseded(lk, local=True)

    async def _save(self, writer, shorter, nchain=-1, full=False):
        """Save the current state.
        """

        async def saver(entry):
            if entry.data is NotGiven:
                return
            res = entry.serialize(nchain=nchain)
            shorter(res)
            await writer(res)

        msg = await self.get_state(nodes=True, known=True, deleted=True)
        # await writer({"info": msg})
        await writer(msg)  # XXX legacy
        await self.root.walk(saver, full=full)

    async def save(self, path: str = None, stream=None, full=True):
        """Save the current state to ``path`` or ``stream``."""
        shorter = PathShortener([])
        async with MsgWriter(path=path, stream=stream) as mw:
            await self._save(mw, shorter, full=full)

    async def save_stream(
        self,
        path: str = None,
        stream: anyio.abc.Stream = None,
        save_state: bool = False,
        done: ValueEvent = None,
        done_val=None,
    ):
        """Save the current state to ``path`` or ``stream``.
        Continue writing updates until cancelled.

        Args:
          path: The file to save to.
          stream: the stream to save to.
          save_state: Flag whether to write the current state.
            If ``False`` (the default), only write changes.
          done: set when writing changes commences, signalling
            that the old save file (if any) may safely be closed.

        Exactly one of ``stream`` or ``path`` must be set.

        This task flushes the current buffer to disk when one second
        passes without updates, or every 100 messages.
        """
        shorter = PathShortener([])

        async with MsgWriter(path=path, stream=stream) as mw:
            msg = await self.get_state(nodes=True, known=True, deleted=True)
            # await mw({"info": msg})
            await mw(msg)  # XXX legacy
            last_saved = time.monotonic()
            last_saved_count = 0

            async with Watcher(self.root, full=True) as updates:
                await self._ready.wait()

                if save_state:
                    await self._save(mw, shorter, full=True)

                await mw.flush()
                if done is not None:
                    await done.set(done_val)

                cnt = 0
                while True:
                    # This dance ensures that we save the system state often enough.
                    t = time.monotonic()
                    td = t - last_saved
                    if td >= 60 or last_saved_count > 1000:
                        msg = await self.get_state(nodes=True, known=True, deleted=True)
                        # await mw({"info": msg})
                        await mw(msg)  # XXX legacy
                        await mw.flush()
                        last_saved = time.monotonic()
                        last_saved_count = 0
                        td = -99999  # translates to something large, below
                        cnt = 0

                    try:
                        async with anyio.fail_after(1 if cnt else 60 - td):
                            msg = await updates.__anext__()
                    except TimeoutError:
                        await mw.flush()
                        cnt = 0
                    else:
                        msg = msg.serialize()
                        shorter(msg)
                        last_saved_count += 1
                        await mw(msg)
                        if cnt >= 100:
                            await mw.flush()
                            cnt = 0
                        else:
                            cnt += 1

    async def _saver(
        self, path: str = None, stream=None, done: ValueEvent = None, save_state=False
    ):

        async with anyio.open_cancel_scope() as s:
            sd = anyio.create_event()
            state = (s, sd)
            self._savers.append(state)
            try:
                await self.save_stream(
                    path=path, stream=stream, done=done, done_val=s, save_state=save_state
                )
            except EnvironmentError as err:
                if done is None:
                    raise
                await done.set_error(err)
            finally:
                async with anyio.open_cancel_scope(shield=True):
                    await sd.set()

    async def run_saver(self, path: str = None, stream=None, save_state=False, wait: bool = True):
        """
        Start a task that continually saves to disk.

        At most one one saver runs at a time; if a new one is started,
        the old saver is cancelled as soon as the new saver's current state
        is on disk (if told to do so) and it is ready to start writing.

        Args:
          path (str): The file to save to. If ``None``, simply stop any
            already-running log.
          stream (anyio.abc.Stream): the stream to save to.
          save_state (bool): Flag whether to write the current state.
            If ``False`` (the default), only write changes.
          wait: wait for the save to really start.

        """
        done = ValueEvent() if wait else None
        res = None
        if path is not None:
            await self.spawn(
                partial(self._saver, path=path, stream=stream, save_state=save_state, done=done)
            )
            if wait:
                res = await done.get()

        # At this point the new saver is operational, so we cancel the old one(s).
        while self._savers is not None and self._savers[0][0] is not res:
            s, sd = self._savers.pop(0)
            await s.cancel()
            await sd.wait()

    async def _sigterm(self):
        with trio.open_signal_receiver(signal.SIGTERM) as r:
            async for s in r:
                for s, sd in self._savers:
                    await s.cancel()
                    await sd.wait()
                break
        os.kill(os.getpid(), signal.SIGTERM)

    @property
    async def is_ready(self):
        """Await this to determine if/when the server is operational."""
        await self._ready.wait()

    @property
    async def is_serving(self):
        """Await this to determine if/when the server is serving clients."""
        await self._ready2.wait()

    async def serve(self, log_path=None, log_inc=False, force=False, ready_evt=None):
        """Task that opens a Serf connection and actually runs the server.

        Args:
          ``setup_done``: optional event that's set when the server is initially set up.
          ``log_path``: path to a binary file to write changes and initial state to.
          ``log_inc``: if saving, write changes, not the whole state.
          ``force``: start up even if entries are missing
        """
        self.force_startup = force
        back = get_backend(self.cfg.server.backend)
        try:
            conn = self.cfg.server[self.cfg.server.backend]
        except KeyError:
            conn = self.cfg.server.connect
        async with back(**conn) as serf:
            # pylint: disable=attribute-defined-outside-init

            # Collect all "info/missing" messages seen since the last
            # healed network split so that they're only sent once.
            self.seen_missing = {}

            # Is the missing-items-sender running?
            # None=no, otherwise flag whether it should run another round
            self.sending_missing = None

            # Nodes which list missing events
            self.fetch_missing = set()

            # Flag whether do_fetch_missing is running (True)
            # or do_send_missing is running (False)
            # or neither (None)
            self.fetch_running = None

            # Set when self.node.tick is no longer None, i.e. we have some
            # reasonable state
            self._ready = anyio.create_event()

            # set when we're ready to accept client connections
            self._ready2 = anyio.create_event()

            self.serf = serf
            self.spawn = serf.spawn

            # Sync recovery steps so that only one node per branch answers
            self._recover_event1 = None
            self._recover_event2 = None

            # local and remote node lists
            self._recover_sources = None

            # Cancel scope; if :meth:`recover_split` is running, use that
            # to cancel
            self._recover_tasks = {}

            # used to sync starting up everything so no messages get either
            # lost, or processed prematurely
            delay = anyio.create_event()
            delay2 = anyio.create_event()
            delay3 = anyio.create_event()

            await self.spawn(self._run_del, delay3)
            await self.spawn(self._delete_also)

            if log_path is not None:
                await self.run_saver(path=log_path, save_state=not log_inc, wait=False)

            # Link up our "user_*" code
            for d in dir(self):
                if d.startswith("user_"):
                    await self.spawn(self.monitor, d[5:], delay)

            await delay3.wait()
            await self.spawn(self.watcher)

            if self._init is not NotGiven:
                assert self.node.tick is None
                self.node.tick = 0
                async with self.next_event() as event:
                    await self.root.set_data(event, self._init, tock=self.tock, server=self)

            await self.spawn(self._sigterm)

            # send initial ping
            await self.spawn(self._pinger, delay2)

            await anyio.sleep(0.1)
            await delay.set()
            await self._check_ticked()  # when _init is set
            await delay2.wait()
            await self._ready.wait()

            cfgs = self.cfg.server.bind
            cfg_b = self.cfg.server.bind_default
            evts = []
            async with anyio.create_task_group() as tg:
                for n, cfg in enumerate(cfgs):
                    cfg = combine_dict(cfg, cfg_b, cls=attrdict)
                    evt = anyio.create_event()
                    evts.append(evt)
                    await tg.spawn(self._accept_clients, cfg, n, evt)
                for evt in evts:
                    await evt.wait()

                await self._ready2.set()
                if ready_evt is not None:
                    await ready_evt.set()
                pass  # end of server taskgroup
            pass  # end of server
        pass  # end of serf client

    async def _accept_clients(self, cfg, n, evt):
        ssl_ctx = gen_ssl(cfg["ssl"], server=True)
        cfg = combine_dict({"ssl": ssl_ctx}, cfg, cls=attrdict)

        async with create_tcp_server(**cfg) as server:
            if n == 0:
                self.ports = server.ports
            if evt is not None:
                await evt.set()

            async for stream in server:
                await self.spawn(self._connect, stream)
            pass  # unwinding create_tcp_server
        pass  # unwinding the client (cancelled or error)

    async def _connect(self, stream):
        c = None
        try:
            c = ServerClient(server=self, stream=stream)
            self._clients.add(c)
            await c.run()
        except (trioBrokenResourceError, anyio.exceptions.ClosedResourceError):
            self.logger.debug("XX %d closed", c._client_nr)
        except BaseException as exc:
            CancelExc = anyio.get_cancelled_exc_class()
            if isinstance(exc, anyio.exceptions.ExceptionGroup):
                # pylint: disable=no-member
                exc = exc.filter(lambda e: None if isinstance(e, CancelExc) else e, exc)
            if exc is not None and not isinstance(exc, CancelExc):
                if isinstance(
                    exc, (trioBrokenResourceError, anyio.exceptions.ClosedResourceError)
                ):
                    self.logger.debug("XX %d closed", c._client_nr)
                else:
                    self.logger.exception("Client connection killed", exc_info=exc)
            try:
                async with anyio.move_on_after(2) as cs:
                    cs.shield = True
                    if c is not None:
                        await c.send({"error": str(exc)})
            except Exception:
                pass
        finally:
            async with anyio.move_on_after(2, shield=True):
                if c is not None:
                    self._clients.remove(c)
                await stream.aclose()
