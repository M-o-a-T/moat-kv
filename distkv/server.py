# Local server
from __future__ import annotations

import trio
from trio.abc import Stream
from async_generator import asynccontextmanager
import msgpack
import trio_serf
from typing import Any
from random import Random
import time
from range_set import RangeSet
import io
from functools import partial

from .model import Entry, NodeEvent, Node, Watcher, UpdateEvent
from .util import attrdict, PathShortener, PathLongener, MsgWriter, MsgReader, Queue, create_tcp_server
from .auth import RootServerUser, loader
from . import client as distkv_client  # needs to be mock-able
from . import _version_tuple

import logging
logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

class ClientError(ValueError):
    """report error to the client but don't dump a stack trace"""
    pass

class _NotGiven:
    pass

_client_nr = 0

def max_n(a,b):
    if a is None:
        return b
    elif b is None:
        return a
    elif a < b:
        return b
    else:
        return a

def cmp_n(a,b):
    if a is None:
        a = -1
    if b is None:
        b = -1
    return b-a

def _multi_response(fn):
    async def wrapper(self, msg, *args):
        seq = msg.seq
        await self.send({'seq':seq, 'state':'start'})
        await fn(self, msg, *args)
        await self.send({'seq':seq, 'state':'end'})
    return wrapper

def _stream(fn):
    """A simple wrapper that calls the original function with each message"""
    async def wrapper(self, msg):
        seq = msg.seq
        q = self.inqueue[seq]
        path = msg.get('path', None)
        longer = PathLongener(path) if path is not None else lambda x:x
        n=0
        while True:
            msg = await q.receive()
            if msg.get('state','') == 'end':
                del self.inqueue[seq]
                await self.send({'seq':seq, 'count':n})
                return
            longer(msg)
            res = await fn(self, msg)
            if isinstance(res, int):
                n += res
    return wrapper


class StreamCommand:
    """Represent the execution of a streamed command.

    Implement the actual command by overriding ``run``.
    Read the next input line by reading ``in_recv_q``.
    
    This auto-detects whether the command sends multiple lines, by closing
    the incoming channel if there's no state=start in the command.

    Selection of outgoing multiline-or-not must be done beforehand,
    by setting ``.multiline``: either statically in a subclass, or
    overriding ``__call__``.
    """
    multiline = False
    send_q = None
    _scope = None
    
    def __new__(cls, client, msg):
        if cls is StreamCommand:
            seq = msg.seq
            cls = globals()["SCmd_"+msg.action]
            return cls(client, msg)
        else:
            return object.__new__(cls)

    def __init__(self, client, msg):
        self.client = client
        self.msg = msg
        self.seq = msg.seq
        self.in_send_q,self.in_recv_q = trio.open_memory_channel(3)

    async def recv(self, msg):
        """Receive another message from the client"""
        s = msg.get('state','')
        if s == 'end':
            await self.in_send_q.aclose()
        else:
            await self.in_send_q.send()

    async def aclose(self):
        await self.in_send_q.aclose()

    async def send(self, **msg):
        """Send a message to the client.

        TODO add a rate limit
        """
        msg['seq'] = self.seq
        if not self.multiline:
            if self.multiline is None:
                raise RuntimeError("Non-Multiline tried to send twice")
            self.multiline = None
        await self.client.send(msg)

    async def __call__(self, **kw):
        msg = self.msg
        if msg.get('state') != 'start':
            # single message
            await self.in_send_q.aclose()

        if self.multiline:
            await self.send(state='start')
            try:
                res = await self.run(**kw)
                if res is not None:
                    await self.send(res)
            except Exception as exc:
                logger.exception("ERS%d: %r", self.seq, self.msg)
                await self.send(error=repr(exc))
            finally:
                await self.send(state='end')

        else:
            res = run(**kw)
            if res is None:
                res = {}
            res['seq'] = self.seq
            await self.send(res)

    async def run(self):
        raise RuntimeError("Do implement me!")


class SCmd_auth(StreamCommand):
    """
    Perform user authorization.

    root: sub-root directory (TODO)
    typ: auth method (_null)
    ident: user identifier (*)

    plus any other data the client-side auth object sends

    This call cannot be used to re-authenticate. The code will go
    through the motions but not actually do anything, thus you can
    non-destructively test an updated authorization.
    """
    multiline = True

    async def run(self):
        msg = self.msg
        client = self.client

        if client._user is not None:
            await client._user.auth_sub(msg)
            return

        root = msg.get('root',None)
        
        if root is not None:
            if self.client.is_chroot:
                raise RuntimeError("You can't auth in a chroot env")
            self.client._chroot(root)

        auth = client.root.follow(None,"auth", nulls_ok=True, create=False)
        if client.user is None:
            a = auth.data['current']
            if msg.typ != a and client.user is None:
                raise RuntimeError("Wrong auth type", a)

        data = auth.follow(msg.typ, 'user', msg.ident, create=False)
        cls = loader(msg.typ,"user",server=True)
        user = await cls.read(data)
        client._user = user
        try:
            await user.auth(self, msg)
            if client.user is None:
                client.user = user
        finally:
            client._user = None


class SCmd_get_tree(StreamCommand):
    """
    Get a subtree.

    path: position to start to enumerate.
    mindepth: tree depth at which to start returning results. Default 0=path location.
    maxdepth: tree depth at which to not go deeper. Default +inf=everything.
    nchain: number of change chain entries to return. Default 0=don't send chain data.

    The returned data is PathShortened.
    """
    multiline = True
    async def run(self):
        msg = self.msg
        entry = self.client.root.follow(*msg.path, create=False, nulls_ok=self.client.nulls_ok)

        kw = {}
        nchain = msg.get('nchain',0)
        ps = PathShortener(entry.path)
        maxdepth = msg.get('maxdepth',None)
        if maxdepth is not None:
            kw['max_depth'] = maxdepth
        mindepth = msg.get('mindepth',None)
        if mindepth is not None:
            kw['min_depth'] = mindepth

        async def send_sub(entry):
            if entry.data is None:
                return
            res = entry.serialize(chop_path=self.client._chop_path, nchain=nchain)
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
        entry = client.root.follow(*msg.path, create=True, nulls_ok=client.nulls_ok)
        seq = msg.seq
        nchain = msg.get('nchain',0)

        async with Watcher(entry) as watcher:
            shorter = PathShortener(entry.path)
            if msg.get('state', False):
                async def worker(entry):
                    res = entry.serialize(chop_path=client._chop_path, nchain=nchain)
                    shorter(res)
                    await self.send(**res)
                await entry.walk(worker)

            async for msg in watcher:
                res = msg.entry.serialize(chop_path=client._chop_path, nchain=nchain)
                shorter(res)
                await self.send(**res)


class ServerClient:
    """Represent one (non-server) client."""
    _nursery = None
    is_chroot = False
    _user = None # user during auth
    user = None # authorized user

    def __init__(self, server: Server, stream: Stream):
        self.server = server
        self.root = server.root
        self.stream = stream
        self.seq = 0
        self.tasks = {}
        self.in_stream = {}
        self._chop_path = 0
        self._send_lock = trio.Lock()

        global _client_nr
        _client_nr += 1
        self._client_nr = _client_nr
        logger.debug("CONNECT %d %s", self._client_nr, repr(stream))

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
        res['seq'] = msg.seq
        await self.send(res)

    async def process(self, msg, *, task_status=trio.TASK_STATUS_IGNORED):
        """
        Process an incoming message.
        """
        if (self.user is None or self._user is not None) and msg.action != "auth":
            raise RuntimeError("You are not authorized")

        seq = msg.seq
        with trio.CancelScope() as s:
            self.tasks[seq] = s
            if 'chain' in msg:
                msg.chain = NodeEvent.deserialize(msg.chain, cache=self.server._nodes, nulls_ok=self.nulls_ok)

            if msg.get('state','') != 'start':
                fn = getattr(self, 'cmd_' + str(msg.action), None)
            if fn is None:
                fn = StreamCommand(self, msg)
                self.in_stream[seq] = fn
            else:
                fn = partial(self._process, fn, msg)
            task_status.started(s)

            try:
                res = await fn()

            except BrokenPipeError as exc:
                logger.error("ERR%d: %s", self._client_nr, repr(exc))

            except Exception as exc:
                if not isinstance(exc, (KeyError,ClientError)):
                    logger.exception("ERR%d: %s", self._client_nr, repr(msg))
                await self.send({'error': str(exc), 'seq': seq})

            finally:
                del self.tasks[seq]
                if isinstance(fn, StreamCommand):
                    del self.in_stream[seq]

    def _chroot(self, root):
        entry = self.root.follow(*root, nulls_ok=False)
        self.root = entry
        self.is_chroot = True
        self._chop_path += len(root)

    async def cmd_root(self, msg):
        """Change to a sub-tree.
        """
        self._chroot(msg.path)
        return entry.serialize(chop_path=self._chop_path)

    async def cmd_get_value(self, msg):
        """Get a node's value.
        """
        if 'node' in msg and 'path' not in msg:
            n = Node(msg.node, cache=self.server._nodes, create=False)
            return n[msg.tick].serialize(chop_path=self._chop_path, nchain=msg.get('nchain', 0))

        try:
            entry = self.root.follow(*msg.path, create=False, nulls_ok=self.nulls_ok)
        except KeyError:
            entry = {'value': None}
        else:
            entry = entry.serialize(chop_path=-1, nchain=msg.get('nchain', 0))
        return entry

    async def cmd_set_value(self, msg, _nulls_ok=False):
        """Set a node's value.
        """
        ## TODO drop this as soon as we have server-side user mods
        if self.user.is_super_root:
            _nulls_ok = 2

        entry = self.root.follow(*msg.path, nulls_ok=_nulls_ok)
        send_prev = True
        if 'prev' in msg:
            if entry.data != msg.prev:
                raise ClientError("Data is %s" % (repr(entry.data),))
            send_prev = False
        if 'chain' in msg:
            if entry.chain is not None:
                if msg.chain is None:
                    raise ClientError("This entry already exists")
                if entry.chain != msg.chain:
                    raise ClientError("Chain is %s" % (repr(entry.chain),))
            elif msg.chain is not None:
                raise ClientError("This entry is new")
            send_prev = False
        res = {'changed': entry.data != msg.value}
        if send_prev:
            res['prev'] = entry.data

        async with self.server.next_event() as event:
            await entry.set_data(event, msg.value, dropped=self.server._dropper)

        nchain = msg.get('nchain', 1)
        if nchain > 0:
            res['chain'] = entry.chain.serialize(nchain=nchain)
        return res

    @_stream
    async def scmd_update(self, msg, q):
        """
        Apply a stored update.

        This streams the input.
        """
        msg = UpdateEvent.deserialize(self.root, msg, nulls_ok=self.nulls_ok)
        await msg.entry.apply(msg, dropped=self._dropper)

    async def cmd_update(self, msg):
        """
        Apply a stored update.

        You usually do this via a stream command.
        """
        msg = UpdateEvent.deserialize(self.root, msg, nulls_ok=self.nulls_ok)
        res = await msg.entry.apply(msg, dropped=self._dropper)
        if res is None:
            return False
        else:
            return res.serialize(chop_path=self._chop_path)

    async def cmd_delete_value(self, msg):
        """Delete a node's value.
        Sub-nodes are not affected.
        """
        if 'value' in msg:
            raise ClientError("A deleted entry can't have a value")
        msg.value = None
        return await self.cmd_set_value(msg)

    def cmd_get_state(self, msg):
        """Return some info about this node's internal state"""
        return self.server.get_state(**msg)

    async def cmd_delete_tree(self, msg):
        """Delete a node's value.
        Sub-nodes are cleared (after their parent).
        """
        seq = msg.seq
        nchain = msg.get('nchain', 0)
        if nchain:
            await self.send({'seq':seq, 'state':'start'})
        ps = PathShortener(msg.path)

        try:
            entry = self.root.follow(*msg.path, nulls_ok=self.nulls_ok)
        except KeyError:
            return False

        async def _del(entry):
            res = 0
            if entry.data is not None:
                async with self.server.next_event() as event:
                    evt = await entry.set_data(event, None, dropped=self.server._dropper)
                    if nchain:
                        r = evt.serialize(chop_path=self._chop_path, nchain=nchain, with_old=True)
                        r['seq'] = seq
                        del r['new_value']  # always None
                        ps(r)
                        await self.send(r)
                res += 1
            for v in entry.values():
                res += await _del(v)
            return res
        res = await _del(entry)
        if nchain:
            await self.send({'seq':seq, 'state':'end'})
        else:
            return {'changed': res}

    async def cmd_log(self, msg):
        await self.server.run_saver(path=msg.path, save_state=msg.get('state', False))
        return True

    async def cmd_save(self, msg):
        await self.server.save(path=msg.path)
        return True

    async def cmd_stop(self, msg):
        try:
            t = self.tasks[msg.task]
        except KeyError:
            return False
        t.cancel()
        return True

    async def cmd_set_auth_typ(self, msg):
        if not self.user.is_super_root:
            raise RuntimeError("You're not allowed to do that")
        a = self.root.follow(None,"auth", nulls_ok=True)
        if a.data is None:
            val = {}
        else:
            val = a.data.copy()

        if msg.typ is None:
            val.pop('current',None)
        elif msg.typ not in a or not len(a[msg.typ]["user"].keys()):
            raise RuntimeError("You didn't configure this method yet")
        else:
            val['current'] = msg.typ
        msg.value = val
        msg.path=(None,"auth")
        return await self.cmd_set_value(msg, _nulls_ok=True)

    async def send(self, msg):
        logger.debug("OUT%d: %s", self._client_nr, msg)
        if self._send_lock is None:
            return
        async with self._send_lock:
            if self._send_lock is None:
                return

            msg['tock'] = self.server.tock
            try:
                await self.stream.send_all(_packer(msg))
            except trio.BrokenResourceError:
                logger.error("Trying to send %r",msg)
                self._send_lock = None
                raise

    async def send_result(self, seq, res):
        res['seq'] = seq
        res['tock'] = self.server.tock
        await self.send(res)

    async def run(self):
        """Main loop for this client connection."""
        unpacker = msgpack.Unpacker(object_pairs_hook=attrdict, raw=False, use_list=False)

        async with trio.open_nursery() as tg:
            self.tg = tg
            msg = {'seq': 0, 'version': _version_tuple,
                    'node':self.server.node.name, 'tick':self.server.node.tick,
                    'tock':self.server.tock}
            try:
                auth = self.root.follow(None,"auth", nulls_ok=True, create=False)
            except KeyError:
                a = None
            else:
                auths = list(auth.keys())
                try:
                    a = auth.data['current']
                except (ValueError,KeyError,IndexError,TypeError):
                    a = None
                else:
                    try:
                        auths.remove(a)
                    except ValueError:
                        a = None
                auths.insert(0,a)
                msg['auth'] = auths
            if a is None:
                self.user = RootServerUser()
            await self.send(msg)

            while True:
                for msg in unpacker:
                    seq = None
                    try:
                        logger.debug("IN %d: %s", self._client_nr, msg)
                        seq = msg.seq
                        send_q = self.in_stream.get(seq, None)
                        if send_q is not None:
                            await send_q.recv(msg)
                        else:
                            if self.seq >= seq:
                                raise ClientError("Sequence numbers are not monotonic: %d < %d" % (self.seq, msg.seq))
                            self.seq = seq
                            await self.tg.start(self.process, msg)
                    except Exception as exc:
                        if not isinstance(exc, ClientError):
                            logger.exception("ERR %d: Client error on %s", self._client_nr, repr(msg))
                        msg = {'error': str(exc)}
                        if seq is not None:
                            msg['seq'] = seq
                        await self.send(msg)

                try:
                    buf = await self.stream.receive_some(4096)
                except (ConnectionResetError, trio.ClosedResourceError,trio.BrokenResourceError):
                    return  # closed/reset/whatever
                if len(buf) == 0:  # Connection was closed.
                    return # done
                unpacker.feed(buf)


class Server:
    serf = None
    _ready = None

    def __init__(self, name: str, cfg: dict, init: Any = _NotGiven, root: Entry = None):
        if root is None:
            root = Entry("ROOT", None)
        self.root = root
        self.cfg = cfg
        self._nodes = {}
        self.node = Node(name, None, cache=self._nodes)
        self._tock = 0
        self._init = init

        self._evt_lock = trio.Lock()
        self._random = Random()

    @asynccontextmanager
    async def next_event(self):
        """A context manager which returns the next event under a lock.

        This increments ``tock`` because that increases the chance that the
        node (or split) where something actually happens wins a collision.
        """
        async with self._evt_lock:
            self.node.tick += 1
            self._tock += 1
            yield NodeEvent(self.node)
            self._tock += 1

    @property
    def tock(self):
        """Retrieve ``tock``.
        
        Also increments it because tock values may not be re-used."""
        self._tock += 1
        return self._tock

    @property
    def random(self):
        """An attribute that generates a random fraction in ``[0,1)``."""
        return self._random.random()

    def _dropper(self, evt, old_evt=_NotGiven):
        """Drop either one event, or any event that is in ``old_evt`` but not in
        ``evt``."""
        if old_evt is None:
            return
        if old_evt is _NotGiven:
            evt.node.supersede(evt.tick)
            return

        nt = {}
        while evt is not None:
            nt[evt.node.name] = evt.tick
            evt = evt.prev
        while old_evt is not None:
            if nt.get(old_evt.node.name, 0) != old_evt.tick:
                old_evt.node.supersede(old_evt.tick)
            old_evt = old_evt.prev


    async def _send_event(self, action, msg, coalesce=False):
        msg['tock'] = self.tock
        if 'node' not in msg:
            msg['node'] = self.node.name
        if 'tick' not in msg:
            msg['tick'] = self.node.tick
        msg = _packer(msg)
        await self.serf.event(self.cfg['root']+'.'+action, msg, coalesce=coalesce)

    async def watcher(self):
        """This method implements the task that watches a (sub)tree for changes"""
        async with Watcher(self.root) as watcher:
            async for msg in watcher:
                if msg.event.node != self.node:
                    continue
                if self.node.tick is None:
                    continue
                p = msg.serialize(nchain=self.cfg['change']['length'])
                await self._send_event('update', p)

    async def get_state(self, nodes=False, known=False, missing=False,
            remote_missing=False, **kw):
        """Return some info about this node's internal state"""
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
        """Process an update."""
        msg = UpdateEvent.deserialize(self.root, msg, cache=self._nodes, nulls_ok=True)
        await msg.entry.apply(msg, dropped=self._dropper)

    async def user_info(self, msg):
        """Process info broadcasts.
        
        These are mainly used in the split recovery protocol."""

        if msg.node == self.node.name:
            return  # ignore our own message

        # 'reason' is the ping chain from the node that triggered this info
        # message. If we're on "our" chain but not on the "other", then we
        # also need to start recovery. Also, if the "other" chain is better,
        # we need to replace ours.
        # This happens when, immediately after a split, our ping is
        # rejected by the remote side. Instead of sending a ping of their
        # own immediately, they send their chain as reason with
        # replace=True.
        r = msg.get('reason', None)
        if r is not None and self._recover_task is None:
            replace = r.get('replace', False)
            pos = self.last_ping_evt.find(self.node)
            rev = NodeEvent.deserialize(r, cache=self._nodes, check_dup=False, nulls_ok=True)
            if replace:
                r.tock = msg.tock
                self.last_ping = r
                self.last_ping_evt = rev
            if pos is not None:
                self.sane_ping = self.last_ping_evt
                await self.spawn(self.recover_split, pos)

        # Step 1
        ticks = msg.get('ticks', None)
        if ticks is not None:
            for n,t in ticks.items():
                n = Node(n, cache=self._nodes)
                n.tick = max_n(n.tick, t)
            if self._recover_event1 is not None and \
                    (self.sane_ping is None or self.node in self.sane_ping):
                logger.debug("Step1 %s: triggered by %s", self.node.name, self.sane_ping.serialize() if self.sane_ping else "-")
                self._recover_event1.set()
            elif self._recover_event1 is not None:
                logger.debug("Step1 %s: not in %s", self.node.name, self.sane_ping.serialize())
#           else:
#               logger.debug("Step1 %s: no event", self.node.name)

        # Step 2
        missing = msg.get('missing', None)
        if missing is not None:
            nn = 0
            for n,k in missing.items():
                n = Node(n, cache=self._nodes)
                r = RangeSet()
                r.__setstate__(k)
                nn += len(r)
                n.reported_missing(r)
                mr = self.seen_missing.get(n, None)
                if mr is None:
                    self.seen_missing[n] = r
                else:
                    mr += r
            if self._recover_event2 is not None and \
                    (self.sane_ping is None or self.node in self.sane_ping):
                logger.debug("Step2 %s: triggered by %s", self.node.name, self.sane_ping.serialize() if self.sane_ping else "-")
                self._recover_event2.set()
            elif self._recover_event2 is not None:
                logger.debug("Step2 %s: not in %s", self.node.name, self.sane_ping.serialize())
#           else:
#               logger.debug("Step2 %s: no event", self.node.name)

            if nn > 0:
                await self._run_send_missing()

        # Step 3
        known = msg.get('known',None)
        if known is not None:
            for n,k in known.items():
                n = Node(n, cache=self._nodes)
                r = RangeSet()
                r.__setstate__(k)
                n.reported_known(r)



    async def user_ping(self, msg):
        """Process ping broadcasts.

        Just queue them for the ``pinger`` task to handle.
        """
        await self.ping_q.put(msg)

    async def monitor(self, action: str, delay: trio.Event = None):
        """The task that hooks to Serf's event stream for receiving messages.
        
        Args:
          ``action``: The action name, corresponding to a Serf ``user_*`` method.
          ``delay``: an optional event to wait for, after starting the
                     listener but before actually processing messages. This
                     helps to avoid possible inconsistency errors on startup.
        """
        cmd = getattr(self, 'user_'+action)
        async with self.serf.stream('user:%s.%s' % (self.cfg['root'], action)) as stream:
            if delay is not None:
                await delay.wait()

            async for resp in stream:
                msg = msgpack.unpackb(resp.payload, object_pairs_hook=attrdict, raw=False, use_list=False)
                self.tock_seen(msg.get('tock',0))
                await cmd(msg)

    def tock_seen(self, tock):
        """Update my current ``tock`` if it's not high enough."""
        self._tock = max(self._tock, tock)

    async def _send_ping(self):
        """Send a ping message and note when to send the next one,
        assuming that no intervening ping arrives.
        """
        self.last_ping_evt = msg = NodeEvent(self.node, prev=self.last_ping_evt)
        self.last_ping = msg = msg.serialize(self.cfg['ping']['length'])
        await self._send_event('ping', msg)

        t = time.time()
        self.next_ping = t + self._time_to_next_ping() * self.cfg['ping']['clock']

    def _time_to_next_ping(self):
        """Calculates the time until sending the next ping is a good idea,
        assuming that none arrive in the meantime, in clocks."""
        if self.last_ping_evt.node == self.node:
            # we transmitted the last ping. If no other ping arrives we are
            # the only node.
            return 3
        # check whether the first half of the ping chain contains nonzero ticks
        # so that if we're not fully up yet, the chain doesn't only consist of
        # nodes that don't work.
        c = self.last_ping_evt.prev
        p = s = 0
        l = 1
        while c is not None:
            if c.tick is not None and c.tick > 0 and p == 0:
                p = l
            if c.node == self.node:
                s = l
            l += 1
            c = c.prev
        if not self._ready.is_set():
            if p > l//2:
                # No it does not. Do not participate.
                return 3

        if s > 0:
            # We are on the chain. Send ping depending on our position.
            return 2 - (s-1)/l
            # this will never be 1 because we need to leave some time for
            # interlopers, below. Otherwise we could divide by l-1, as
            # l must be at least 2. s must also be at least 1.

        if l < self.cfg['ping']['length']-1:
            # the chain is too short. Try harder to get onto it.
            f = 3
        else:
            f = 10
        if self.random < 1/f/len(self._nodes):
            # send early (try getting onto the chain)
            return 1+self.random/len(self._nodes)
        else:
            # send late (fallback)
            return 2.5+self.random/2

    async def pinger(self, delay: Event):
        """
        This task
        * sends PING messages
        * handles incoming pings
        * triggers split recovery

        The initial ping is delayed randomly.

        Args:
          ``delay``: an event to set after the initial ping message has
                     been sent.
        """
        clock = self.cfg['ping']['clock']

        # initial delay: anywhere from clock/2 to clock seconds
        await trio.sleep((self.random/2+0.5)*clock)
        await self._send_ping()
        delay.set()

        while True:
            msg = None
            t = max(self.next_ping - time.time(), 0)
            #logger.debug("S %s: wait %s", self.node.name, t)
            with trio.move_on_after(t):
                msg = await self.ping_q.get()
            if msg is None:
                await self._send_ping()
                continue

            # Handle incoming ping
            event = NodeEvent.deserialize(msg, cache=self._nodes, check_dup=False, nulls_ok=True)

            if self.node == event.node:
                # my own message, returned
                continue

            if event.prev is not None and self.last_ping_evt.equals(event.prev):
                # valid "next" ping
                self.last_ping = msg
                self.last_ping_evt = event
                self.next_ping = time.time() + clock * self._time_to_next_ping()
                continue

            saved_ping = self.last_ping_evt
            # colliding pings.
            #
            # This while loop is only used as a "goto forward".
            # ``break`` == "the new ping is better"
            # ``pass``  == "the last ping I saw is better"
            while True:
                if event.tick is None and self._ready.is_set():
                    # always prefer our ping
                    break
                if event.tick is not None and not self._ready.is_set():
                    # always prefer the other ping
                    pass
                else:
                    if msg.tock < self.last_ping.tock:
                        break
                    if msg.tock == self.last_ping.tock:
                        if cmp_n(event.tick, self.last_ping_evt.tick) < 0:
                            break
                        if cmp_n(event.tick, self.last_ping_evt.tick) == 0:
                            if event.node.name < self.last_ping_evt.node.name:
                                break
                            assert event.node.name != self.last_ping_evt.node.name

                # If we get here, the other ping is "better".
                logger.debug("Coll Ack %s: %s",self.node.name,msg)
                self.last_ping = msg
                self.last_ping_evt = event
                t = time.time()
                self.next_ping = time.time() + clock * self._time_to_next_ping()
                break  # always terminate the loop

            if event.prev is not None and event.prev.equals(self.last_ping_evt):
                # These pings refer to the same previous ping. Good.
                logger.debug("Coll PRE %s: %s",self.node.name,msg)
                continue

            if self.last_ping is not msg:
                logger.debug("Coll NO  %s: %s",self.node.name,msg)


            # We either have a healed network split (bad) or are new (oh well).
            if self._ready.is_set():
                # otherwise I have nothing to say

                if event.tick is None:
                    # The colliding node does not have data. Ignore.
                    continue

                if saved_ping.tick is None:
                    # The node sending the last ping had no data, so
                    # there's no collision now.
                    continue

                pos = saved_ping.find(self.node)
                if pos is not None:
                    if self._recover_task is None:
                        # await self._recover_task.cancel()
                        if self.sane_ping is None:
                            self.sane_ping = saved_ping
                        await self.spawn(self.recover_split, pos, self.last_ping is not msg)
            elif self.fetch_running is None and self.last_ping_evt.tick is not None:
                await self.spawn(self.fetch_data)

    async def _get_host_port(self, node):
        """Retrieve the remote system to connect to"""
        port = self.cfg['server']['port']
        domain = self.cfg['domain']
        host = node.name
        if domain is not None:
            host += '.'+domain
        return (host,port)

    async def do_send_missing(self):
        """Task to periodically send "missing …" messages
        """
        logger.debug("send-missing %s started", self.node.name)
        clock = self.cfg['ping']['clock']/2
        while self.fetch_missing:
            if self.fetch_running is not False:
                logger.debug("send-missing %s halted", self.node.name)
                return
            clock *= self.random/2+1
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
            await self._send_event('info', msg)

        logger.debug("send-missing %s ended", self.node.name)
        if self.node.tick is None:
            self.node.tick = 0
            await self._check_ticked()
        self.fetch_running = None

    async def fetch_data(self):
        """
        We are newly started and don't have any data.

        Try to get the initial data from some other node.
        """
        if self.fetch_running is not None:
            return
        self.fetch_running = True
        while True:
            n = self.last_ping_evt
            while n is not None:
                node = n.node
                n = n.prev
                if node.tick is None:  # not ready
                    continue

                try:
                    host, port = await self._get_host_port(node)
                    async with distkv_client.open_client(host, port) as client:

                        res = await client.request('get_tree', iter=True, from_server=self.node.name, nchain=999, path=())
                        async for r in res:
                            r = UpdateEvent.deserialize(self.root, r, cache=self._nodes, nulls_ok=True)
                            await r.entry.apply(r, dropped=self._dropper)
                        self.tock_seen(res.end_msg.tock)

                        res = await client.request('get_state', nodes=True, from_server=self.node.name, known=True, iter=False)
                        await self._process_info(res)

                except (AttributeError, KeyError, ValueError, AssertionError, TypeError):
                    raise
                except Exception as exc:
                    logger.exception("Unable to connect to %s" % (node,))
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
                        self.node.tick = 0
                        self.fetch_running = None
                        await self._check_ticked()
                    return
            await trio.sleep(self.cfg['ping']['clock']*1.1)

    async def _process_info(self, msg):
        for nn,t in msg.get('nodes',{}).items():
            nn = Node(nn, cache=self._nodes)
            nn.tick = max_n(nn.tick,t)
        for nn,k in msg.get('known',{}).items():
            nn = Node(nn, cache=self._nodes)
            r = RangeSet()
            r.__setstate__(k)
            nn.reported_known(r, local=True)
        for nn,k in msg.get('remote_missing',{}).items():
            # used when loading data from a state file
            nn = Node(nn, cache=self._nodes)
            r = RangeSet()
            r.__setstate__(k)
            nn.reported_missing(r)

    async def _check_ticked(self):
        if self._ready is None:
            return
        if self.node.tick is not None:
            logger.debug("Ready %s",self.node.name)
            self._ready.set()

    async def recover_split(self, pos, replace=False):
        """
        Recover from a network split.
        """
        clock = self.cfg['ping']['clock']
        tock = self.tock
        self._recover_tock = tock
        self._recover_event1 = trio.Event()
        self._recover_event2 = trio.Event()
        logger.info("SplitRecover %s: %s @%d", self.node.name, pos, tock)

        with trio.CancelScope() as s:
            self._recover_task = s

            try:
                # Step 1: send an info/ticks message
                # for pos=0 this fires immediately. That's intentional.
                with trio.move_on_after(clock * (1-1/(1<<pos))/2) as x:
                    await self._recover_event1.wait()
                if self.sane_ping is None:
                    logger.info("SplitRecover %s: no sane 1", self.node.name)
                    return
                if x.cancel_called:
                    logger.info("SplitRecover %s: no signal 1", self.node.name)
                    msg = dict((x.name,x.tick) for x in self._nodes.values())

                    msg = attrdict(ticks=msg)
                    msg.reason = self.sane_ping.serialize()
                    msg.reason.replace = replace

                    await self._send_event('info', msg)

                # Step 2: send an info/missing message
                # for pos=0 this fires after clock/2, so that we get a
                # chance to wait for other info/ticks messages. We can't
                # trigger on them because there may be more than one, for a
                # n-way merge.
                with trio.move_on_after(clock * (2-1/(1<<pos))/2) as x:
                    await self._recover_event2.wait()

                if x.cancel_called:
                    logger.info("SplitRecover %s: no signal 2", self.node.name)
                    msg = dict()
                    for n in self._nodes.values():
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
                    await self._send_event('info', msg)

                # wait a bit more before continuing. Again this depends on
                # `pos` so that there won't be two nodes that send the same
                # data at the same time, hopefully.
                await trio.sleep(clock * (1-1/(1<<pos)))

                # Step 3: start a task that sends stuff
                await self._run_send_missing()

            finally:
                # Protect against cleaning up when another recovery task has
                # been started (because we saw another merge)
                if self._recover_tock != tock:
                    logger.info("SplitRecover %s: canceled @%d", self.node.name, tock)
                    return
                logger.info("SplitRecover %s: finished @%d", self.node.name, tock)
                self._recover_tock = 0
                self._recover_task = None
                self._recover_event1 = None
                self._recover_event2 = None
                self.sane_ping = None
                self.seen_missing = {}

    async def _run_send_missing(self):
        """Start :meth:`_send_missing_data` if it's not running"""

        pos = (self.sane_ping or self.last_ping_evt).find(self.node)
        if self.sending_missing is None:
            self.sending_missing = True
            await self.spawn(self._send_missing_data, pos)
        elif not self.sending_missing:
            self.sending_missing = True

    async def _send_missing_data(self, pos):
        """Step 3 of the re-join protocol.
        For each node, collect events that somebody has reported as missing,
        and re-broadcast them. If the event is unavailable, send a "known"
        message.
        """
        clock = self.cfg['ping']['clock']
        if pos is None:
            await trio.sleep(clock*(1/2+self.random/5))
        else:
            await trio.sleep(clock*(1-1/(1<<pos))/2)

        while self.sending_missing:
            self.sending_missing = False
            nodes = list(self._nodes.values())
            self._random.shuffle(nodes)
            known = {}
            for n in nodes:
                k = RangeSet()
                for r in n.remote_missing & n.local_known:
                    for t in range(*r):
                        if t not in n.remote_missing:
                            # some other node could have sent this while we worked
                            await trio.sleep(self.cfg['ping']['clock']/10)
                            continue
                        if t in n:
                            msg = n[t].serialize()
                            await self._send_event('update', msg)
                        else:
                            k.add(t)
                if k:
                    known[n.name] = k.__getstate__()
                rm = n.remote_missing
                rm -= n.local_known
                assert rm is n.remote_missing
            if known:
                await self._send_event('info', attrdict(known=known))
        self.sending_missing = None

    async def load(self, path:str, stream: io.IOBase = None, local: bool = False):
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
                if 'value' in m:
                    longer(m)
                    m = UpdateEvent.deserialize(self.root, m, cache=self._nodes, nulls_ok=True)
                    await m.entry.apply(m, local=local, dropped=self._dropper)
                elif 'nodes' in m or 'known' in m:
                    await self._process_info(m)
                else:
                    logger.warn("Unknown message in stream: %s", repr(m))
        logger.info("Loading finished.")
        
    async def _save(self, writer, shorter, nchain=99):
        """Save the current state.

        TODO: Add code for snapshotting.
        """
        async def saver(entry):
            res = entry.serialize(nchain=nchain)
            shorter(res)
            await writer(res)
        msg = await self.get_state(nodes=True, known=True)
        await writer(msg)
        await self.root.walk(saver)

    async def save(self, path:str=None, stream=None, delay:Event = None):
        """Save the current state to ``path`` or ``stream``."""
        shorter = PathShortener([])
        async with MsgWriter(path=path, stream=stream) as mw:
            await self._save(mw, shorter)

    async def save_stream(self, path:str=None, stream=None, save_state:bool=False, done:int=None):
        """Save the current state to ``path`` or ``stream``.
        Continue writing updates until cancelled.
        """
        shorter = PathShortener([])

        async with MsgWriter(path=path, stream=stream) as mw:
            async with Watcher(self.root) as updates:
                await self._ready.wait()

                if save_state:
                    await self._save(mw, shorter)

                msg = await self.get_state(nodes=True, known=True)
                await mw(msg)
                await mw.flush()
                if done is not None:
                    await done.set()

                async for msg in updates:
                    await mw(msg.serialize())
    
    _saver_prev = None
    async def _saver(self, path:str, done, save_state=False):

        with trio.CancelScope() as s:
            self._saver_prev = s
            try:
                await self.save_stream(path=path, done=done, save_state=save_state)
            finally:
                if self._saver_prev is s:
                    self._saver_prev = None

    async def run_saver(self, path: str=None, stream=None, save_state=False):
        """Start a task that continually saves to disk.

        Only one saver can run at a time; if a new one is started, 
        the old one is stopped as soon as the new saver's current state is on disk.
        """
        done = trio.Event()
        s = self._saver_prev
        await self.spawn(self._saver, path=path, stream=stream, done=done)
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

    async def serve(self, log_stream = None, task_status=trio.TASK_STATUS_IGNORED):
        """Task that opens a Serf connection and actually runs the server.
        
        Args:
          ``setup_done``: optional event that's set when the server is initially set up.
          ``log_stream``: a binary stream to write changes and initial state to.
        """
        async with trio_serf.serf_client(**self.cfg['serf']) as serf:
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

            # Queue for processing incoming ping broadcasts
            self.ping_q = Queue(self.cfg['ping'].length+2)

            # Last "reasonable" ping seen
            self.last_ping = None
            self.last_ping_evt = None
            self.last_sent_ping = None

            # Last ping on "our" branch when a network split is healed
            self.sane_ping = None

            # Sync recovery steps so that only one node per branch answers
            self._recover_event1 = None
            self._recover_event2 = None

            # Cancel scope; if :meth:`recover_split` is running, use that
            # to cancel
            self._recover_task = None
            self._recover_tock = 0

            # used to sync starting up everything so no messages get either
            # lost, or processed prematurely
            delay = trio.Event()
            delay2 = trio.Event()

            if self.cfg['state'] is not None:
                await self.spawn(self.save, self.cfg['state'])

            if log_stream is not None:
                await self.run_saver(stream=log_stream, save_state=True)

            # Link up our "user_*" code
            for d in dir(self):
                if d.startswith("user_"):
                    await self.spawn(self.monitor, d[5:], delay)

            await self.spawn(self.watcher)

            if self._init is not _NotGiven:
                assert self.node.tick is None
                self.node.tick = 0
                async with self.next_event() as event:
                    await self.root.set_data(event, self._init)

            # send initial ping
            await self.spawn(self.pinger, delay2)
            await delay2.wait()

            await trio.sleep(0.1)
            delay.set()
            await self._check_ticked()  # when _init is set

            cfg_s = self.cfg['server'].copy()
            cfg_s.pop('host', 'localhost')

            await self._ready.wait()
            if cfg_s.get('port',_NotGiven) is None:
                del cfg_s['port']
            async with create_tcp_server(**cfg_s) as server:
                self.ports = server.ports
                task_status.started(server)
            
                logger.debug("S %s: opened %s", self.node.name, self.ports)
                self._ready2.set()
                async for client in server:
                    await self.spawn(self._connect, client)
                pass # unwinding create_tcp_server
            pass # unwinding serf_client (cancelled or error)

    async def _connect(self, stream):
        c = ServerClient(server=self, stream=stream)
        try:
            await c.run()
        except BaseException as exc:
            if isinstance(exc, trio.MultiError):
                exc = exc.filter(trio.Cancelled)
            if exc is not None:
                logger.exception("Client connection killed", exc_info=exc)
            try:
                with trio.move_on_after(2) as cs:
                    cs.shield = True
                    await self.send({'error': str(exc)})
            except Exception:
                pass
        finally:
            await stream.aclose()

