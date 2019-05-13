# Local server
from __future__ import annotations

import trio
import anyio
from trio.abc import Stream
from async_generator import asynccontextmanager
import msgpack
import asyncserf
from typing import Any
from range_set import RangeSet
import io
from functools import partial
from asyncserf.util import CancelledError as SerfCancelledError
from asyncserf.actor import Actor, GoodNodeEvent, RecoverEvent, RawPingEvent, PingEvent
from collections import defaultdict

# from trio_log import LogStream

from .model import Entry, NodeEvent, Node, Watcher, UpdateEvent, NodeSet
from .types import RootEntry, ConvNull
from .core_actor import CoreActor
from .default import CFG
from .codec import packer, unpacker
from .util import (
    attrdict,
    PathShortener,
    PathLongener,
    MsgWriter,
    MsgReader,
    combine_dict,
    create_tcp_server,
    gen_ssl,
    num2byte,
    byte2num,
    NotGiven,
)
from .exceptions import ClientError, NoAuthError, CancelledError
from . import client as distkv_client  # needs to be mock-able
from . import _version_tuple

import logging

logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack


_client_nr = 0


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
    Read the next input line by reading ``in_recv_q``.

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
            cls = globals()["SCmd_" + msg.action]
            return cls(client, msg)
        else:
            return object.__new__(cls)

    def __init__(self, client, msg):
        self.client = client
        self.msg = msg
        self.seq = msg.seq
        self.in_send_q, self.in_recv_q = trio.open_memory_channel(3)
        self.client.in_stream[self.seq] = self

    async def received(self, msg):
        """Receive another message from the client"""

        s = msg.get("state", "")
        err = msg.get("error", None)
        if err:
            await self.in_send_q.send(msg)
        if s == "end":
            self.end_msg = msg
            await self.aclose()
        elif not err:
            await self.in_send_q.send(msg)

    async def aclose(self):
        self.client.in_stream.pop(self.seq, None)
        await self.in_send_q.aclose()

    async def recv(self):
        msg = await self.in_recv_q.receive()

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
        await self.client.send(msg)

    async def __call__(self, **kw):
        msg = self.msg
        if msg.get("state") != "start":
            # single message
            await self.in_send_q.aclose()

        if self.multiline > 0:
            await self.send(state="start")
            try:
                res = await self.run(**kw)
                if res is not None:
                    await self.send(**res)
            except Exception as exc:
                if not isinstance(exc, SerfCancelledError):
                    self.client.logger.exception("ERS%d: %r", self.seq, self.msg)
                await self.send(error=repr(exc))
            finally:
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

        root = msg.get("root", ())
        auth = client.root.follow(*root, None, "auth", nulls_ok=2, create=False)
        if client.user is None:
            a = auth.data["current"]
            if msg.typ != a and client.user is None:
                raise RuntimeError("Wrong auth type", a)

        data = auth.follow(msg.typ, "user", msg.ident, create=False)

        cls = loader(msg.typ, "user", server=True)
        user = cls.load(data)
        client._user = user
        try:
            await user.auth(self, msg)

            if client.user is None:
                client._chroot(root)
                client.user = user

                client.conv = user.aux_conv(client.root)
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
        if root and not self.user.is_super_root:
            raise RuntimeError("Cannot read tenant users")
        kind = msg.get("kind", "user")

        auth = client.root.follow(*root, None, "auth", nulls_ok=2, create=False)
        if "ident" in msg:
            data = auth.follow(msg.typ, kind, msg.ident, create=False)
            await self.send_one(data, nchain=nchain)

        else:
            d = auth.follow(msg.typ, kind, create=False)
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

    multiline = True

    async def run(self):
        from .auth import loader

        msg = self.msg
        client = self.client
        if not client.user.can_auth_read:
            raise RuntimeError("Not allowed")

        root = msg.get("root", ())
        if root and not self.user.is_super_root:
            raise RuntimeError("Cannot read tenant users")
        kind = msg.get("kind", "user")

        auth = client.root.follow(*root, None, "auth", nulls_ok=2, create=False)
        data = auth.follow(msg.typ, kind, msg.ident, create=False)
        cls = loader(msg.typ, kind, server=True, make=True)
        user = cls.load(data)
        return await user.send(self)


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
        if root and not self.user.is_super_root:
            raise RuntimeError("Cannot write tenant users")
        kind = msg.get("kind", "user")

        client.root.follow(*root, None, "auth", nulls_ok=2, create=True)
        cls = loader(msg.typ, kind, server=True, make=True)

        user = await cls.recv(self, msg)
        msg.value = user.save()
        msg.path = (*root, None, "auth", msg.typ, kind, user.ident)
        msg.chain = user._chain
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

    async def run(self):
        msg = self.msg
        client = self.client
        entry = client.root.follow(*msg.path, create=False, nulls_ok=client.nulls_ok)

        kw = {}
        nchain = msg.get("nchain", 0)
        ps = PathShortener(entry.path)
        max_depth = msg.get("max_depth", None)
        conv = client.conv

        if max_depth is not None:
            kw["max_depth"] = max_depth
        min_depth = msg.get("min_depth", None)
        if min_depth is not None:
            kw["min_depth"] = min_depth

        async def send_sub(entry):
            if entry.data is NotGiven:
                return
            res = entry.serialize(chop_path=client._chop_path, nchain=nchain, conv=conv)
            ps(res)
            await self.send(**res)

        await entry.walk(send_sub, **kw)


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
        entry = client.root.follow(*msg.path, create=True, nulls_ok=client.nulls_ok)
        nchain = msg.get("nchain", 0)
        max_depth = msg.get("max_depth", -1)
        min_depth = msg.get("min_depth", 0)

        async with Watcher(entry) as watcher:
            async with anyio.create_task_group() as tg:
                tock = client.server.tock
                shorter = PathShortener(entry.path)
                if msg.get("fetch", False):

                    async def orig_state():
                        kv = {"max_depth": max_depth, "min_depth": min_depth}

                        async def worker(entry):
                            if entry.tock < tock:
                                res = entry.serialize(
                                    chop_path=client._chop_path,
                                    nchain=nchain,
                                    conv=conv,
                                )
                                shorter(res)
                                await self.send(**res)

                        await entry.walk(worker, **kv)
                        await self.send(state="uptodate")

                    await tg.spawn(orig_state)

                async for m in watcher:
                    ml = len(m.entry.path) - len(msg.path)
                    if ml < min_depth:
                        continue
                    if max_depth > 0 and ml > max_depth:
                        continue
                    res = m.entry.serialize(
                        chop_path=client._chop_path, nchain=nchain, conv=conv
                    )
                    shorter(res)
                    await self.send(**res)


class SCmd_serfmon(StreamCommand):
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
        raw = msg.get("raw", False)

        if msg.type[0] == ":":
            raise RuntimeError("Types may not start with a colon")

        async with self.client.server.serf.stream("user:" + msg.type) as stream:
            async for resp in stream:
                res = attrdict(type=msg.type)
                if raw:
                    res["raw"] = resp.data
                else:
                    try:
                        res["data"] = msgpack.unpackb(
                            resp.data,
                            object_pairs_hook=attrdict,
                            raw=False,
                            use_list=False,
                        )
                    except Exception as exc:
                        res["raw"] = resp.data
                        res["error"] = repr(exc)

                await self.send(**res)


class SCmd_update(StreamCommand):
    """
    Stream a stored update to the server and apply it.
    """

    multiline = False

    async def run(self):
        client = self.client
        conv = client.conv
        tock_seen = client.server.tock_seen
        msg = self.msg

        path = msg.get("path", None)
        longer = PathLongener(path) if path is not None else lambda x: x
        n = 0
        async for msg in self.in_recv_q:
            longer(msg)
            msg = UpdateEvent.deserialize(
                client.root, msg, nulls_ok=client.nulls_ok, conv=conv
            )
            await tock_seen(msg.get("tock", None))
            await msg.entry.apply(msg, server=self, root=self.root)
            n += 1
        await self.send(count=n)


class ServerClient:
    """Represent one (non-server) client."""

    _nursery = None
    is_chroot = False
    _user = None  # user during auth
    user = None  # authorized user
    _dh_key = None
    conv = ConvNull

    def __init__(self, server: "Server", stream: Stream):
        self.server = server
        self.root = server.root
        self.metaroot = self.root.follow(None, create=True, nulls_ok=True)
        self.stream = stream
        self.seq = 0
        self.tasks = {}
        self.in_stream = {}
        self._chop_path = 0
        self._send_lock = trio.Lock()

        global _client_nr
        _client_nr += 1
        self._client_nr = _client_nr
        self.logger = server.logger
        self.logger.debug("CONNECT %d %s", self._client_nr, repr(stream))

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

        seq = msg.seq
        async with anyio.open_cancel_scope() as s:
            self.tasks[seq] = s
            if "chain" in msg:
                msg.chain = NodeEvent.deserialize(
                    msg.chain, cache=self.server._nodes, nulls_ok=self.nulls_ok
                )

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
                self.logger.error("ERR%d: %s", self._client_nr, repr(exc))

            except Exception as exc:
                if not isinstance(exc, ClientError):
                    self.logger.exception("ERR%d: %s", self._client_nr, repr(msg))
                await self.send({"error": str(exc), "seq": seq})

            finally:
                del self.tasks[seq]

    def _chroot(self, root):
        if not root:
            return
        entry = self.root.follow(*root, nulls_ok=False)
        self.root = entry
        self.is_chroot = True
        self._chop_path += len(root)

    async def cmd_diffie_hellman(self, msg):
        if self._dh_key:
            raise RuntimeError("Can't call dh twice")
        from diffiehellman.diffiehellman import DiffieHellman

        def gen_key():
            length = msg.get("length", 1024)
            k = DiffieHellman(key_length=length, group=(5 if length < 32 else 18))
            k.generate_public_key()
            k.generate_shared_secret(byte2num(msg.pubkey))
            self._dh_key = num2byte(k.shared_secret)[0:32]
            return k

        k = await trio.run_sync_in_worker_thread(
            gen_key, limiter=self.server.crypto_limiter
        )
        return {"pubkey": num2byte(k.public_key)}

    cmd_diffie_hellman.noAuth = True

    @property
    def dh_key(self):
        if self._dh_key is None:
            raise RuntimeError("The client has not executed DH key exchange")
        return self._dh_key

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

    async def cmd_root(self, msg):
        """Change to a sub-tree.
        """
        self._chroot(msg.path)
        return self.root.serialize(chop_path=self._chop_path, conv=self.conv)

    async def cmd_get_internal(self, msg):
        return await self.cmd_get_value(msg, root=self.metaroot)

    async def cmd_set_internal(self, msg):
        return await self.cmd_set_value(msg, root=self.metaroot)

    async def cmd_delete_internal(self, msg):
        return await self.cmd_delete_value(msg, root=self.metaroot)

    async def cmd_get_tock(self, msg):
        return {"tock": self.server.tock}

    async def cmd_get_value(self, msg, _nulls_ok=None, root=None):
        """Get a node's value.
        """
        if "node" in msg and "path" not in msg:
            n = Node(msg.node, cache=self.server._nodes, create=False)
            return n[msg.tick].serialize(
                chop_path=self._chop_path, nchain=msg.get("nchain", 0), conv=self.conv
            )

        if _nulls_ok is None:
            _nulls_ok = self.nulls_ok
        if root is None:
            root = self.root
        try:
            entry = root.follow(*msg.path, create=False, nulls_ok=_nulls_ok)
        except KeyError:
            entry = {}
            if msg.get("nchain", 0):
                entry["chain"] = None
        else:
            entry = entry.serialize(
                chop_path=-1, nchain=msg.get("nchain", 0), conv=self.conv
            )
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
        entry = root.follow(*msg.path, nulls_ok=_nulls_ok)
        if root is self.root and "match" in self.metaroot:
            try:
                self.metaroot["match"].check_value(
                    None if value is NotGiven else value, entry
                )
            except ClientError:
                raise
            except Exception as exc:
                raise ClientError(repr(exc)) from None
                # TODO pass exceptions to the client

        send_prev = True
        if "prev" in msg:
            if entry.data != msg.prev:
                raise ClientError("Data is %s" % (repr(entry.data),))
            send_prev = False
        if "chain" in msg:
            if msg.chain is None:
                if entry.data is not NotGiven:
                    raise ClientError("This entry already exists")
            elif entry.data is not NotGiven:
                if entry.chain != msg.chain:
                    raise ClientError("Chain is %s" % (repr(entry.chain),))
            else:
                raise ClientError("This entry is new")
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
                NotGiven
                if value is NotGiven
                else self.conv.dec_value(value, entry=entry),
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
            self.root, msg, nulls_ok=self.nulls_ok, conv=self.conv
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
            n = Node(n, None, cache=self.server._nodes)
            r = RangeSet()
            r.__setstate__(v)
            for a, b in r:
                for t in range(a, b):
                    if t not in n:
                        deleted.add(n.name, t)
        if deleted:
            await self._send_event("info", attrdict(deleted=deleted.__getstate__()))

    async def cmd_get_state(self, msg):
        """Return some info about this node's internal state"""
        return await self.server.get_state(**msg)

    async def cmd_serfsend(self, msg):
        if msg.type[0] == ":":
            raise RuntimeError("Types may not start with a colon")
        if "raw" in msg:
            assert "data" not in msg
            data = msg.raw
        else:
            data = _packer(msg.data)
        await self.server.serf.event(
            msg.type, data, coalesce=msg.get("coalesce", False)
        )

    async def cmd_delete_tree(self, msg):
        """Delete a node's value.
        Sub-nodes are cleared (after their parent).
        """
        seq = msg.seq
        nchain = msg.get("nchain", 0)
        if nchain:
            await self.send({"seq": seq, "state": "start"})
        ps = PathShortener(msg.path)

        try:
            entry = self.root.follow(*msg.path, nulls_ok=self.nulls_ok)
        except KeyError:
            return False

        async def _del(entry):
            res = 0
            if entry.data is not None:
                async with self.server.next_event() as event:
                    evt = await entry.set_data(
                        event,
                        NotGiven,
                        server=self,
                        tock=self.server.tock,
                    )
                    if nchain:
                        r = evt.serialize(
                            chop_path=self._chop_path,
                            nchain=nchain,
                            with_old=True,
                            conv=self.conv,
                        )
                        r["seq"] = seq
                        del r["new_value"]  # always None
                        ps(r)
                        await self.send(r)
                res += 1
            for v in entry.values():
                res += await _del(v)
            return res

        res = await _del(entry)
        if nchain:
            await self.send({"seq": seq, "state": "end"})
        else:
            return {"changed": res}

    async def cmd_log(self, msg):
        await self.server.run_saver(path=msg.path, save_state=msg.get("fetch", False))
        return True

    async def cmd_save(self, msg):
        await self.server.save(path=msg.path)
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
        a = self.root.follow(None, "auth", nulls_ok=True)
        if a.data is NotGiven:
            val = {}
        else:
            val = a.data.copy()

        if msg.typ is None:
            val.pop("current", None)
        elif msg.typ not in a or not len(a[msg.typ]["user"].keys()):
            raise RuntimeError(
                "You didn't configure this method yet:" + repr((msg.typ, vars(a)))
            )
        else:
            val["current"] = msg.typ
        msg.value = val
        msg.path = (None, "auth")
        return await self.cmd_set_value(msg, _nulls_ok=True)

    async def send(self, msg):
        self.logger.debug("OUT%d: %s", self._client_nr, msg)
        if self._send_lock is None:
            return
        async with self._send_lock:
            if self._send_lock is None:
                return

            if "tock" not in msg:
                msg["tock"] = self.server.tock
            try:
                await self.stream.send_all(_packer(msg))
            except trio.BrokenResourceError:
                self.logger.error("Trying to send %d %r", self._client_nr, msg)
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
        unpacker = msgpack.Unpacker(
            object_pairs_hook=attrdict, raw=False, use_list=False
        )

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
                auth = self.root.follow(None, "auth", nulls_ok=True, create=False)
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
                for msg in unpacker:
                    seq = None
                    try:
                        self.logger.debug("IN %d: %s", self._client_nr, msg)
                        seq = msg.seq
                        send_q = self.in_stream.get(seq, None)
                        if send_q is not None:
                            await send_q.received(msg)
                        else:
                            if self.seq >= seq:
                                raise ClientError(
                                    "Channel closed? Sequence error: %d < %d"
                                    % (self.seq, msg.seq)
                                )
                            self.seq = seq
                            evt = anyio.create_event()
                            await self.tg.spawn(self.process, msg, evt)
                            await evt.wait()
                    except Exception as exc:
                        if not isinstance(exc, ClientError):
                            self.logger.exception(
                                "ERR %d: Client error on %s", self._client_nr, repr(msg)
                            )
                        msg = {"error": str(exc)}
                        if seq is not None:
                            msg["seq"] = seq
                        await self.send(msg)

                try:
                    buf = await self.stream.receive_some(4096)
                except (
                    ConnectionResetError,
                    trio.ClosedResourceError,
                    trio.BrokenResourceError,
                ):
                    return  # closed/reset/whatever
                if len(buf) == 0:  # Connection was closed.
                    return  # done
                unpacker.feed(buf)


class _RecoverControl:
    _id = 0

    def __init__(self, server, scope, prio, local_history, sources):
        self.server = server
        self.scope = scope
        self.prio = prio
        self.local_history = local_history
        self.sources = sources
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
        self.scope.cancel()
        rt = self.server._recover_tasks
        for node in self.local_history:
            if rt.get(node, None) is self:
                del rt[node]
        self.local_history = ()
        for evt in list(self._waiters.values()):
            evt.set()

    async def set(self, n):
        evt = self._waiters.get(n, None)
        if evt is None:
            evt = trio.Event()
            self._waiters[n] = evt
        evt.set()

    async def wait(self, n):
        evt = self._waiters.get(n, None)
        if evt is None:
            evt = trio.Event()
            self._waiters[n] = evt
        await evt.wait()


class Server:
    """
    This is the DistKV server. It manages connections to the Serf server,
    its clients, and (optionally) a file that logs all changes.
    """

    serf = None
    _ready = None
    _actor = None
    _core_actor = None

    def __init__(self, name: str, cfg: dict, init: Any = NotGiven, root: Entry = None):
        self._tock = 0
        if root is None:
            root = RootEntry(self, tock=self.tock)
        self.root = root
        self.cfg = combine_dict(cfg, CFG)
        self.paranoid_root = root if self.cfg["paranoia"] else None
        self._nodes = {}
        self.node = Node(name, None, cache=self._nodes)
        self._init = init
        self.crypto_limiter = trio.CapacityLimiter(3)
        self.logger = logging.getLogger("distv.server." + name)
        self._delete_also_nodes = NodeSet()

        self._evt_lock = trio.Lock()
        self._clients = set()

    @asynccontextmanager
    async def next_event(self):
        """A context manager which returns the next event under a lock.

        This increments ``tock`` because that increases the chance that the
        node (or split) where something actually happens wins a collision.
        """
        async with self._evt_lock:
            self.node.tick += 1
            self._tock += 1
            await self._set_tock()
            yield NodeEvent(self.node)
            self._tock += 1
            await self._set_tock()

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

    async def core_check(self, value):
        """
        Called when ``(None,"core")`` is set.
        """
        if value is NotGiven:
            await self._core_actor.disable()
            return

        nodes = value.get("nodes", ())
        if self.node.name in nodes:
            await self._core_actor.enable(len(nodes))
        else:
            await self._core_actor.disable(len(nodes))

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

    async def _send_event(self, action: str, msg: dict, coalesce=False):
        """
        Helper to send a Serf event to the ``action`` endpoint.

        Args:
          action (str): the endpoint to send to. Prefixed by ``cfg.root``.
          msg: the message to send.
          coalesce (bool): Flag whether old messages may be thrown away.
            Default: ``False``.
        """
        if "tock" not in msg:
            msg["tock"] = self.tock
        else:
            await self.tock_seen(msg["tock"])
        if "node" not in msg:
            msg["node"] = self.node.name
        if "tick" not in msg:
            msg["tick"] = self.node.tick
        omsg = msg
        msg = _packer(msg)
        await self.serf.event(self.cfg["root"] + "." + action, msg, coalesce=coalesce)

    async def watcher(self):
        """
        The background task that watches a (sub)tree for changes.
        """
        async with Watcher(self.root) as watcher:
            async for msg in watcher:
                if msg.event.node != self.node:
                    continue
                if self.node.tick is None:
                    continue
                p = msg.serialize(nchain=self.cfg["change"]["length"])
                await self._send_event("update", p)

    async def resync_deleted(self, nodes):
        """
        Owch. We need to re-sync.

        We collect the latest ticks in our object tree and send them to one
        of the core nodes.
        """

        for n in nodes:
            try:
                host, port = await self._get_host_port(n)
                async with distkv_client.open_client(host, port) as client:
                    # TODO auth this client
                    nodes = NodeSet()
                    n_nodes = 0

                    async def send_nodes():
                        nonlocal nodes, n_nodes
                        res = await client._request(
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

#           except (AttributeError, KeyError, ValueError, AssertionError, TypeError):
#               raise
            except Exception:
                raise
#               self.logger.exception("Unable to connect to %s" % (nodes,))
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
            n = Node(n, cache=self._nodes)
            n.purge_deleted(v)

    async def get_state(
        self,
        nodes=False,
        known=False,
        deleted=False,
        missing=False,
        remote_missing=False,
        **kw
    ):
        """
        Return some info about this node's internal state.
        """
        res = attrdict()
        if nodes:
            nd = res.nodes = {}
            for n in self._nodes.values():
                nd[n.name] = n.tick
        if known:
            nd = res.known = {}
            for n in self._nodes.values():
                lk = n.local_known
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
        return res

    async def user_update(self, msg):
        """
        Process an update message: deserialize it and apply the result.
        """
        msg = UpdateEvent.deserialize(self.root, msg, cache=self._nodes, nulls_ok=True)
        await msg.entry.apply(msg, server=self, root=self.paranoid_root, )

    async def user_info(self, msg):
        """
        Process info broadcasts.

        These messages are mainly used by the split recovery protocol.
        """

        if msg.node == self.node.name:
            return  # ignore our own message

        # Step 1
        ticks = msg.get("ticks", None)
        if ticks is not None:
            for n, t in ticks.items():
                n = Node(n, cache=self._nodes)
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
                n = Node(n, cache=self._nodes)
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
                await self._run_send_missing(None)

        # Step 3
        known = msg.get("known", None)
        if known is not None:
            for n, k in known.items():
                n = Node(n, cache=self._nodes)
                r = RangeSet()
                r.__setstate__(k)
                n.report_known(r)

        deleted = msg.get("deleted", None)
        if deleted is not None:
            for n, r in deleted.items():
                n.report_deleted(r, self)

    async def _delete_also(self):
        """
        Add deletion records to the core actor.
        """
        while True:
            await trio.sleep(10)
            if self._delete_also_nodes:
                self._core_actor.add_deleted(self._delete_also_nodes)
                self._delete_also_nodes = NodeSet()

    async def monitor(self, action: str, delay: trio.Event = None):
        """
        The task that hooks to Serf's event stream for receiving messages.

        Args:
          action (str): The action name, corresponding to a Serf ``user_*`` method.
          delay (trio.Event): an optional event to wait for, after starting the
            listener but before actually processing messages. This helps to
            avoid consistency problems on startup.
        """
        cmd = getattr(self, "user_" + action)
        try:
            async with self.serf.stream(
                "user:%s.%s" % (self.cfg["root"], action)
            ) as stream:
                if delay is not None:
                    await delay.wait()

                async for resp in stream:
                    msg = msgpack.unpackb(
                        resp.payload, object_pairs_hook=attrdict, raw=False, use_list=False
                    )
                    await self.tock_seen(msg.get("tock", 0))
                    await cmd(msg)
        except (CancelledError, SerfCancelledError):
            pass

    async def _run_core(self, evt):
        try:
            self._core_actor = CoreActor(self)
            await self._core_actor.run(evt=evt)
        finally:
            self._core_actor = None

    async def _pinger(self, delay: trio.Event):
        """
        This task
        * sends PING messages
        * handles incoming pings
        * triggers split recovery

        The initial ping is delayed randomly.

        Args:
          delay (trio.Event): an event to set after the initial ping
            message has been sent.
        """
        cfg = self.cfg["ping"]
        async with anyio.create_task_group() as tg:
            async with Actor(
                client=self.serf,
                prefix=self.cfg["root"] + ".ping",
                name=self.node.name,
                cfg=cfg,
                tg=tg,
                packer=packer, unpacker=unpacker,
            ) as actor:
                self._actor = actor
                await self._check_ticked()
                delay.set()
                async for msg in actor:
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
                    elif isinstance(msg, RawPingEvent):
                        msg = msg.msg
                        msg_node = msg["node"] if "node" in msg else msg["history"][0]
                        val = msg.get("value", None)
                        if val is not None:
                            await self.tock_seen(val[0])
                            val = val[1]
                        else:
                            val = 0
                        Node(msg_node, val, cache=self._nodes)

    async def _get_host_port(self, host):
        """Retrieve the remote system to connect to"""
        port = self.cfg["server"]["port"]
        domain = self.cfg["domain"]
        if domain is not None:
            host += "." + domain
        return (host, port)

    async def do_send_missing(self):
        """Task to periodically send "missing …" messages
        """
        self.logger.debug("send-missing started")
        clock = self.cfg["ping"]["gap"]
        while self.fetch_missing:
            if self.fetch_running is not False:
                self.logger.debug("send-missing halted")
                return
            clock *= self._actor.random / 2 + 1
            await trio.sleep(clock)

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
            await self._send_event("info", msg)

        self.logger.debug("send-missing ended")
        if self.node.tick is None:
            self.node.tick = 0
            await self._check_ticked()
        self.fetch_running = None

    async def fetch_data(self, nodes):
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
                async with distkv_client.open_client(host, port) as client:
                    # TODO auth this client

                    res = await client._request(
                        "get_tree",
                        iter=True,
                        from_server=self.node.name,
                        nchain=-1,
                        path=(),
                    )
                    async for r in res:
                        r = UpdateEvent.deserialize(
                            self.root, r, cache=self._nodes, nulls_ok=True
                        )
                        await r.entry.apply(
                            r, server=self, root=self.paranoid_root
                        )
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
                self.logger.exception("Unable to connect to %s" % (nodes,))
            else:
                # At this point we successfully cloned some other
                # node's state, so we now need to find whatever that
                # node didn't have.

                for n in self._nodes.values():
                    if n.tick and len(n.local_missing):
                        self.fetch_missing.add(n)
                if len(self.fetch_missing):
                    self.fetch_running = False
                    await self.spawn(self.do_send_missing)
                else:
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
        # nodes: list of known nodes and their max ticks
        for nn, t in msg.get("nodes", {}).items():
            nn = Node(nn, cache=self._nodes)
            nn.tick = max_n(nn.tick, t)

        # known: per-node range of ticks that have been resolved
        for nn, k in msg.get("known", {}).items():
            nn = Node(nn, cache=self._nodes)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_known(r, local=True)

        # deleted: per-node range of ticks that have been deleted
        deleted = msg.get("deleted", {})
        for nn, k in deleted.items():
            nn = Node(nn, cache=self._nodes)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_deleted(r, add=add)

        # remote_missing: per-node range of ticks that should be re-sent
        # This is used when loading data from a state file
        for nn, k in msg.get("remote_missing", {}).items():
            nn = Node(nn, cache=self._nodes)
            r = RangeSet()
            r.__setstate__(k)
            nn.report_missing(r)

    async def _check_ticked(self):
        if self._ready is None:
            return
        if self.node.tick is not None:
            self.logger.debug("Ready")
            self._ready.set()
            await self._set_tock()

    async def recover_split(self, prio, replace, local_history, sources):
        """
        Recover from a network split.
        """
        with trio.CancelScope() as scope:
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
                clock = self.cfg["ping"]["cycle"]
                tock = self.tock
                self.logger.debug("SplitRecover: %s @%d", prio, tock)

                # Step 1: send an info/ticks message
                # for prio=0 this fires immediately. That's intentional.
                with trio.move_on_after(clock * (1 - 1 / (1 << prio))) as x:
                    await t.wait(1)
                if x.cancel_called:
                    if prio > 0:
                        self.logger.debug("SplitRecover: no signal 1")
                    msg = dict((x.name, x.tick) for x in self._nodes.values())

                    msg = attrdict(ticks=msg)
                    await self._send_event("info", msg)

                # Step 2: send an info/missing message
                # for prio=0 this fires after clock/2, so that we get a
                # chance to wait for other info/ticks messages. We can't
                # trigger on them because there may be more than one, for a
                # n-way merge.
                with trio.move_on_after(clock * (2 - 1 / (1 << prio)) / 2) as x:
                    await t.wait(2)

                if x.cancel_called:
                    if prio > 0:
                        self.logger.debug("SplitRecover: no signal 2")
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

                    msg = attrdict(missing=msg)
                    await self._send_event("info", msg)

                # wait a bit more before continuing. Again this depends on
                # `prio` so that there won't be two nodes that send the same
                # data at the same time, hopefully.
                await trio.sleep(clock * (1 - 1 / (1 << prio)))

                # Step 3: start a task that sends stuff
                await self._run_send_missing(prio)

            finally:
                # Protect against cleaning up when another recovery task has
                # been started (because we saw another merge)
                self.logger.debug("SplitRecover %d: finished @%d", t._id, t.tock)
                self.seen_missing = {}
                await t.cancel()

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
        clock = self.cfg["ping"]["cycle"]
        if prio is None:
            await trio.sleep(clock * (1 + self._actor.random / 3))
        else:
            await trio.sleep(
                clock * (1 - (1 / (1 << prio)) / 2 - self._actor.random / 5)
            )

        while self.sending_missing:
            self.sending_missing = False
            nodes = list(self._nodes.values())
            self._actor._rand.shuffle(nodes)
            known = {}
            deleted = {}
            for n in nodes:
                k = RangeSet()
                for r in n.remote_missing & n.local_known:
                    for t in range(*r):
                        if t not in n.remote_missing:
                            # some other node could have sent this while we worked
                            await trio.sleep(self.cfg["ping"]["gap"] / 3)
                            continue
                        if t in n:
                            msg = n[t].serialize()
                            await self._send_event("update", msg)
                            n.remote_missing.discard(t)
                        elif t not in n.local_deleted:
                            k.add(t)
                if k:
                    known[n.name] = k.__getstate__()
                assert not (n.remote_missing & n.local_known)

                d = n.remote_missing & n.local_deleted
                if d:
                    deleted[n.name] = d.__getstate__()

            if known or deleted:
                await self._send_event("info", attrdict(known=known, deleted=deleted))
        self.sending_missing = None

    async def load(
        self, path: str = None, stream: io.IOBase = None, local: bool = False
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
                    m = UpdateEvent.deserialize(
                        self.root, m, cache=self._nodes, nulls_ok=True
                    )
                    await self.tock_seen(m.tock)
                    await m.entry.apply(
                        m, local=local, server=self, root=self.paranoid_root
                    )
                elif "nodes" in m or "known" in m or "deleted" in m:
                    await self._process_info(m)
                else:
                    self.logger.warning("Unknown message in stream: %s", repr(m))
        self.logger.debug("Loading finished.")

    async def _save(self, writer, shorter, nchain=-1):
        """Save the current state.

        TODO: Add code for snapshotting.
        """

        async def saver(entry):
            res = entry.serialize(nchain=nchain)
            shorter(res)
            await writer(res)

        msg = await self.get_state(nodes=True, known=True, deleted=True)
        await writer(msg)
        await self.root.walk(saver)

    async def save(self, path: str = None, stream=None, delay: trio.Event = None):
        """Save the current state to ``path`` or ``stream``."""
        shorter = PathShortener([])
        async with MsgWriter(path=path, stream=stream) as mw:
            await self._save(mw, shorter)

    async def save_stream(
        self,
        path: str = None,
        stream=None,
        save_state: bool = False,
        done: trio.Event = None,
    ):
        """Save the current state to ``path`` or ``stream``.
        Continue writing updates until cancelled.
        """
        shorter = PathShortener([])

        async with MsgWriter(path=path, stream=stream) as mw:
            async with Watcher(self.root) as updates:
                await self._ready.wait()

                if save_state:
                    await self._save(mw, shorter)

                msg = await self.get_state(nodes=True, known=True, deleted=True)
                await mw(msg)
                await mw.flush()
                if done is not None:
                    await done.set()

                async for msg in updates:
                    await mw(msg.serialize())

    _saver_prev = None

    async def _saver(self, path: str, done, save_state=False):

        with trio.CancelScope() as s:
            self._saver_prev = s
            try:
                await self.save_stream(path=path, done=done, save_state=save_state)
            finally:
                if self._saver_prev is s:
                    self._saver_prev = None

    async def run_saver(self, path: str = None, stream=None, save_state=False):
        """Start a task that continually saves to disk.

        Only one saver can run at a time; if a new one is started,
        the old one is stopped as soon as the new saver's current state is on disk.
        """
        done = trio.Event()
        s = self._saver_prev
        await self.spawn(
            self._saver, path=path, stream=stream, save_state=save_state, done=done
        )
        await done.wait()
        if s is not None:
            await s.cancel()

    @property
    async def is_ready(self):
        """Await this to determine if/when the server is operational."""
        await self._ready.wait()

    @property
    async def is_serving(self):
        """Await this to determine if/when the server is serving clients."""
        await self._ready2.wait()

    async def serve(
        self, log_path=None, task_status=trio.TASK_STATUS_IGNORED, ready_evt=None
    ):
        """Task that opens a Serf connection and actually runs the server.

        Args:
          ``setup_done``: optional event that's set when the server is initially set up.
          ``log_path``: path to a binary file to write changes and initial state to.
        """
        async with asyncserf.serf_client(**self.cfg["serf"]) as serf:
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
            self._ready = trio.Event()

            # set when we're ready to accept client connections
            self._ready2 = trio.Event()

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
            delay = trio.Event()
            delay2 = trio.Event()
            delay3 = trio.Event()

            await self.spawn(self._run_core, delay3)
            await self.spawn(self._delete_also)

            if self.cfg["state"] is not None:
                await self.spawn(self.save, self.cfg["state"])

            if log_path is not None:
                await self.run_saver(path=log_path, save_state=True)

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

            # send initial ping
            await self.spawn(self._pinger, delay2)

            await trio.sleep(0.1)
            delay.set()
            await self._check_ticked()  # when _init is set
            await delay2.wait()

            cfg_s = self.cfg["server"].copy()
            cfg_s.setdefault("host", "localhost")
            ssl_ctx = cfg_s.pop("ssl", False)
            ssl_ctx = gen_ssl(ssl_ctx, server=True)

            await self._ready.wait()
            if cfg_s.get("port", NotGiven) is None:
                del cfg_s["port"]
            async with create_tcp_server(**cfg_s) as server:
                self.ports = server.ports
                task_status.started(server)
                if ready_evt is not None:
                    await ready_evt.set()

                self.logger.debug("S: opened %s", self.ports)
                self._ready2.set()
                async for client in server:
                    if ssl_ctx:
                        # client = LogStream(client,"SL")
                        client = trio.SSLStream(client, ssl_ctx, server_side=True)
                        # client = LogStream(client,"SH")
                        await client.do_handshake()
                    await self.spawn(self._connect, client)
                pass  # unwinding create_tcp_server
            pass  # unwinding serf_client (cancelled or error)

    async def _connect(self, stream):
        c = ServerClient(server=self, stream=stream)
        try:
            self._clients.add(c)
            await c.run()
        except BaseException as exc:
            if isinstance(exc, trio.MultiError):
                exc = exc.filter(trio.Cancelled)
            if exc is not None and type(exc) is not trio.Cancelled:
                self.logger.exception("Client connection killed", exc_info=exc)
            try:
                with trio.move_on_after(2) as cs:
                    cs.shield = True
                    await self.send({"error": str(exc)})
            except Exception:
                pass
        finally:
            self._clients.remove(c)
            await stream.aclose()
