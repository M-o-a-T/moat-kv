"""
Client code.

Main entry point: :func:`open_client`.
"""

import anyio
import outcome
import msgpack
import socket
import weakref
import heapq
import random

try:
    from contextlib import asynccontextmanager, AsyncExitStack
except ImportError:
    from async_generator import asynccontextmanager
    from async_exit_stack import AsyncExitStack

from asyncserf.util import ValueEvent
from .util import attrdict, gen_ssl, num2byte, byte2num, PathLongener, NoLock, NotGiven
from .exceptions import (
    ClientAuthMethodError,
    ClientAuthRequiredError,
    ServerClosedError,
    ServerConnectionError,
    ServerError,
    CancelledError,
)

import logging

logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

__all__ = ["NoData", "ManyData", "open_client", "StreamedRequest"]


class NoData(ValueError):
    """No reply arrived"""


class ManyData(ValueError):
    """More than one reply arrived"""


@asynccontextmanager
async def open_client(host, port, init_timeout=5, auth=None, ssl=None):
    """
    This async context manager returns an opened client connection.

    There is no attempt to reconnect if the Serf connection should fail.
    """
    client = Client(host, port, ssl=ssl)
    async with anyio.create_task_group() as tg:
        async with client._connected(
            tg, init_timeout=init_timeout, auth=auth
        ) as client:
            yield client


class ClientEntry:
    """A helper class that represents a node on the server, as returned by
    :meth:`Client.mirror`.
    """

    def __init__(self, parent, name=None):
        self._children = dict()
        self._path = parent._path + (name,)
        self._name = name
        self.value = None
        self.chain = None
        self._root = weakref.ref(parent.root)
        self.client = parent.client
        self._lock = anyio.create_lock()  # for saving etc.

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is "same as this class".
        """
        return cls

    @property
    def root(self):
        return self._root()

    @property
    def subpath(self):
        """Return the path to this entry, starting with its :class:`ClientRoot` base."""
        return self._path[len(self.root._path) :]  # noqa: E203

    @property
    def all_children(self):
        """Iterate all child nodes.
        You can send ``True`` to the iterator if you want to skip a subtree.
        """
        for k in self:
            res = (yield k)
            if res is True:
                continue
            yield from iter(k)

    def get(self, name):
        """
        Returns the child named "name". It is created (locally) if it doesn't exist.

        Arguments:
          name (str): The child node's name.
        """
        try:
            c = self._children[name]
        except KeyError:
            c = self.child_type(name)(self, name)
            self._children[name] = c
        return c

    def __getitem__(self, k):
        return self._children[k]

    def __iter__(self):
        """Iterating an entry returns its children."""
        return iter(list(self._children.values()))

    def __contains__(self, k):
        if isinstance(k, type(self)):
            k = k._name
        return k in self._children

    async def update(self, value, _locked=False, nchain=0):
        """Update this node's value.

        This is a coroutine.
        """
        async with NoLock if _locked else self._lock:
            r = await self.root.client.set(
                *self._path, chain=self.chain, value=value, nchain=nchain
            )
            self.value = value
            return r

    async def add(self, name, value):
        """Add a child node with that value.

        Arguments:
          name (str): the name of the sub-node
          value: the node's value

        This is a coroutine.
        """
        return await self.client.set(*self._path, name, chain=None, value=value)

    async def set_value(self, value=NotGiven):
        """Callback to set the value when data has arrived.
        
        This method is strictly for overriding.
        Don't call me, I'll call you.

        This is a coroutine, for ease of integration.
        """
        if value is not NotGiven:
            self.value = value
        elif hasattr(self, "value"):
            del self.value


def _node_gt(self, other):
    if other is None:
        return True
    if self == other:
        return False
    while self["node"] != other["node"]:
        self = self["prev"]
        if self is None:
            return False
    return self["tick"] >= other["tick"]


class AttrClientEntry(ClientEntry):
    """A ClientEntry which expects a dict as value and sets (some of) the clients'
    attributes appropriately.

    Set the classvar ``ATTRS`` to a list of the attrs you want saved. Note
    that these are not inherited: when you subclass, copy and extend the
    ``ATTRS`` of your superclass.

    If the entry is deleted (value set to ``None``, the attributes listed in
    ``ATTRS`` will be deleted too, or revert to the class values.
    """

    ATTRS = ()

    async def update(self, val):
        raise RuntimeError("Nope. Set attributes and call '.save()'.")

    async def set_value(self, val=NotGiven):
        """Callback to set the value when data has arrived.

        This method sets the actual attributes.

        This method is strictly for overriding.
        Don't call me, I'll call you.
        """
        await super().set_value(val)
        if val is None:
            for k in self.ATTRS:
                try:
                    delattr(self, k)
                except AttributeError:
                    pass
            return
        for k in self.ATTRS:
            if k in val:
                setattr(self, k, val[k])

    async def save(self, wait=False):
        """
        Save myself to storage, by copying ATTRS to a new value.
        """
        res = {}
        async with self._lock:
            for attr in type(self).ATTRS:
                try:
                    v = getattr(self, attr)
                except AttributeError:
                    pass
                else:
                    res[attr] = v
            r = await super().update(value=res, _locked=True, nchain=3 if wait else 0)
            if wait:
                await self.root.wait_chain(r.chain)
            return r


class ClientRoot(ClientEntry):
    """This class represents the root of a subsystem's storage.
    
    To use this class, create a subclass that, at minimum, overrides
    ``CFG`` and ``child_type``. ``CFG`` must be a dict with at least a
    ``prefix`` tuple. You instantiate the entry using :meth:`as_handler`.

    """

    CFG = "You need to override this with a dict(prefix=('where','ever'))"

    def __init__(self, client, *path, need_wait=False, cfg=None):
        self._children = dict()
        self.client = client
        self._path = path
        self.value = None
        self._need_wait = need_wait
        self._loaded = anyio.create_event()
        if cfg is None:
            cfg = {}
        self._cfg = cfg
        self._name = cfg.get("name", self.client.name)

        if need_wait:
            self._waiters = dict()
            self._seen = dict()

    @classmethod
    async def as_handler(cls, client, cfg={}):
        """Return a (or "the") instance of this class.
        
        The handler is created if it doesn't exist.

        INstances are distinguished by their prefix (from config).
        """
        c = {}
        c.update(cls.CFG)
        c.update(cfg)

        def make():
            return client.mirror(*c["prefix"], root_type=cls, need_wait=True, cfg=c)

        return await client.unique_helper(*c["prefix"], factory=make)

    @classmethod
    def child_type(cls, name):
        """Given a node, return the type which the child with that name should have.
        The default is :cls:`ClientEntry`.
        """
        return ClientEntry

    @property
    def root(self):
        """Returns this instance."""
        return self

    def follow(self, *path, create=True, unsafe=False):
        """Look up a sub-entry.
        
        Arguments:
          *path (str): the path elements to follow.
          create (bool): Create the entries. Default ``True``. If
            ``False``, raise ` KeyError`` if an entry does not exist.
          unsafe (bool): Allow a single path element that's a tuple.
            This usually indicates a mistake by the caller. Defaults to
            ``False``. Please try not to need this.

        The path may not be empty. It also must not be a one-element list,
        because that indicates that you called ``.follow(path)`` instead of
        ``.follow(*path)``. To allow that, set ``unsafe``, though a better
        idea is to structure your data that this is not necessary.
        """
        if not unsafe and len(path) == 1 and isinstance(path, (list, tuple)):
            raise RuntimeError("You seem to have used 'path' instead of '*path'.")

        node = self
        for elem in path:
            if create:
                node = node.get(elem)
            else:
                node = node[elem]

        if not unsafe and node is self:
            raise RuntimeError("Empty path")
        return node

    async def run_starting(self):
        """Hook for 'about to start reading'"""
        pass

    async def running(self):
        """Hook for 'done reading current state'"""
        await self._loaded.set()

    @asynccontextmanager
    async def run(self):
        """A coroutine that fetches, and continually updates, a subtree.
        """
        async with anyio.create_task_group() as tg:
            self._tg = tg

            async def monitor():
                pl = PathLongener(())
                await self.run_starting()
                async with self.client._stream(
                    "watch", nchain=3, path=self._path, fetch=True
                ) as w:

                    async for r in w:
                        if "path" not in r:
                            if r.get("state", "") == "uptodate":
                                await self.running()
                            continue
                        pl(r)
                        entry = self.follow(*r.path, create=True, unsafe=True)
                        try:
                            assert _node_gt(r.chain, entry.chain)
                        except AttributeError:
                            pass
                        await entry.set_value(r.get("value", NotGiven))
                        entry.chain = r.get("chain", None)

                        if not self._need_wait or "chain" not in r:
                            continue
                        c = r.chain
                        while c is not None:
                            try:
                                if self._seen[c.node] < c.tick:
                                    self._seen[c.node] = c.tick
                            except KeyError:
                                self._seen[c.node] = c.tick
                            try:
                                w = self._waiters[c.node]
                            except KeyError:
                                pass
                            else:
                                while w and w[0][0] <= c.tick:
                                    await heapq.heappop(w)[1].set()
                            c = c.get("prev", None)

            await tg.spawn(monitor)
            try:
                yield self
            finally:
                await tg.cancel_scope.cancel()

    async def cancel(self):
        """Stop the monitor"""
        await self._tg.cancel_scope.cancel()

    async def wait_loaded(self):
        """Wait for the tree to be loaded completely."""
        await self._loaded.wait()

    async def wait_chain(self, chain):
        """Wait for a tree update containing this tick."""
        try:
            if chain.tick <= self._seen[chain.node]:
                return
        except KeyError:
            pass
        w = self._waiters.setdefault(chain.node, [])
        e = anyio.create_event()
        heapq.heappush(w, (chain.tick, e))
        await e.wait()

    def spawn(self, *a, **kw):
        return self._tg.spawn(*a, **kw)


class StreamedRequest:
    """
    This class represents a bidirectional multi-message request.

    stream: True if you need to send a multi-message request.
            Set to None if you already sent a single-message request.
    report_start: True if the initial state=start message of a multi-reply
                  should be included in the iterator.
                  If False, only available as ``.start_msg``.
    TODO: add rate limit.

    Call ``.send(**params)`` to send something; call ``.recv()``
    or async-iterate for receiving.
    """

    start_msg = None
    end_msg = None

    def __init__(self, client, seq, stream: bool = False, report_start: bool = False):
        self._stream = stream
        self._client = client
        self.seq = seq
        self._stream = stream
        self.q = anyio.create_queue(100)
        self._client._handlers[seq] = self
        self._reply_stream = None
        self.n_msg = 0
        self._report_start = report_start
        # None: no message yet; True: begin seen; False: end or single message seen

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        self.n_msg += 1
        if "error" in msg:
            if self.q is not None:
                await self.q.put(outcome.Error(ServerError(msg.error)))
            return
        state = msg.get("state", "")

        if state == "start":
            if self._reply_stream is not None:
                raise RuntimeError("Recv state 2", self._reply_stream, msg)
            self._reply_stream = True
            self.start_msg = msg
            if self._report_start:
                await self.q.put(outcome.Value(msg))

        elif state == "end":
            if self._reply_stream is not True:
                raise RuntimeError("Recv state 3", self._reply_stream, msg)
            self._reply_stream = None
            self.end_msg = msg
            if self.q is not None:
                await self.q.put(None)
                self.q = None
            return False

        else:
            if state not in ("", "uptodate"):
                logger.warning("Unknown state: %s", msg)

            if self._reply_stream is False:
                raise RuntimeError("Recv state 1", self._reply_stream, msg)
            elif self._reply_stream is None:
                self._reply_stream = False
            if self.q is not None:
                await self.q.put(outcome.Value(msg))
                if self._reply_stream is False:
                    await self.q.put(None)
                    self.q = None

    async def get(self):
        """Receive a single reply"""
        pass  # receive reply
        if self._reply_stream:
            raise RuntimeError("Unexpected multi stream msg")
        msg = await self.recv()
        if self._reply_stream or self.n_msg != 1:
            raise RuntimeError("Unexpected multi stream msg")
        return msg

    def __iter__(self):
        raise RuntimeError("You need to use 'async for …'")

    __next__ = __iter__

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.q is None:
            raise StopAsyncIteration
        res = await self.q.get()
        if res is None:
            raise StopAsyncIteration
        try:
            return res.unwrap()
        except CancelledError:
            raise StopAsyncIteration

    async def send(self, **params):
        # logger.debug("Send %s", params)
        if not self._stream:
            if self._stream is None:
                raise RuntimeError("You can't send more than one request")
            self._stream = None
        elif self._stream is True:
            self._stream = 2
            params["state"] = "start"
        elif self._stream == 2 and params.get("state", "") == "end":
            self._stream = None
        await self._client._send(seq=self.seq, **params)

    async def recv(self):
        return await self.__anext__()

    async def cancel(self):
        if self.q is not None:
            await self.q.put(outcome.Error(CancelledError()))
        await self.aclose()

    async def aclose(self, timeout=0.2):
        if self.q is not None:
            await self.q.put(None)
            self.q = None
        if self._stream == 2:
            await self._client._send(seq=self.seq, state="end")
            if timeout is not None:
                async with anyio.move_on_after(timeout):
                    try:
                        await self.recv()
                    except StopAsyncIteration:
                        return
            req = await self._client._request(action="stop", task=self.seq, _async=True)
            return await req.get()


class _SingleReply:
    """
    This class represents a single-message reply.
    It will delegate itself to a StreamedRequest if a multi message reply
    arrives.
    """

    def __init__(self, conn, seq):
        self._conn = conn
        self.seq = seq
        self.q = ValueEvent()

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        if msg.get("state") == "start":
            res = StreamedRequest(self._conn, self.seq, stream=None)
            await res.set(msg)
            await self.q.set(res)
            return res
        elif "error" in msg:
            await self.q.set_error(ServerError(msg.error))
        else:
            await self.q.set(msg)
        return False

    async def get(self):
        """Wait for and return the result.

        This is a coroutine.
        """
        return await self.q.get()

    async def cancel(self):
        pass


class Client:
    """
    The client side of a DistKV connection.

    Use :func:`open_client` to use this class.
    """

    _server_init = None  # Server greeting
    _dh_key = None
    exit_stack = None

    def __init__(self, host, port, ssl=False, name=None):
        self.host = host
        self.port = port
        self._seq = 0
        self._handlers = {}
        self._send_lock = anyio.create_lock()
        self.ssl = gen_ssl(ssl, server=False)
        self._helpers = {}
        if name is None:
            name = "".join(random.choices("abcdefghjkmnopqrstuvwxyz23456789", k=9))
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def node(self):
        return self._server_init["node"]

    async def get_tock(self):
        """Fetch the next tock value from the server."""
        m = await self._request("get_tock")
        return m.tock

    async def unique_helper(self, *path, factory=None):
        """
        Run a (single) async context manager on that path.

        """
        h = self._helpers.get(path, None)
        if h is None:
            h = anyio.create_lock()
            self._helpers[path] = h
        if isinstance(h, anyio.abc.Lock):
            async with h:

                async def _run(factory, evt):
                    async with factory() as f:
                        nonlocal h
                        h = f
                        await evt.set()
                        while True:
                            await anyio.sleep(99999)

                evt = anyio.create_event()
                await self.tg.spawn(_run, factory, evt)
                await evt.wait()
                self._helpers[path] = h
        return h

    async def _handle_msg(self, msg):
        try:
            seq = msg.seq
        except AttributeError:
            if "error" in msg:
                raise RuntimeError("Server error", msg.error)
            raise RuntimeError("Reader got out of sync: " + str(msg))
        try:
            hdl = self._handlers[seq]
        except KeyError:
            logger.warning("Spurious message %s: %s", seq, msg)
            return

        res = await hdl.set(msg)
        if res is False:
            del self._handlers[seq]
        elif res:
            self._handlers[seq] = res

    async def dh_secret(self, length=1024):
        """Exchange a diffie-hellman secret with the server"""
        if self._dh_key is None:
            from diffiehellman.diffiehellman import DiffieHellman

            def gen_key():
                k = DiffieHellman(key_length=length, group=(5 if length < 32 else 18))
                k.generate_public_key()
                return k

            k = await anyio.run_in_thread(gen_key)
            res = await self._request(
                "diffie_hellman", pubkey=num2byte(k.public_key), length=length
            )  # length=k.key_length
            await anyio.run_in_thread(k.generate_shared_secret, byte2num(res.pubkey))
            self._dh_key = num2byte(k.shared_secret)[0:32]
        return self._dh_key

    async def _send(self, **params):
        async with self._send_lock:
            await self._socket.send_all(_packer(params))

    async def _reader(self, *, evt=None):
        """Main loop for reading
        """
        unpacker = msgpack.Unpacker(
            object_pairs_hook=attrdict, raw=False, use_list=False
        )

        async with anyio.open_cancel_scope() as s:
            if evt is not None:
                await evt.set()
            try:
                while True:
                    for msg in unpacker:
                        # logger.debug("Recv %s", msg)
                        try:
                            await self._handle_msg(msg)
                        except anyio.exceptions.ClosedResourceError:
                            raise RuntimeError(msg)

                    if self._socket is None:
                        break
                    try:
                        buf = await self._socket.receive_some(4096)
                    except anyio.exceptions.ClosedResourceError:
                        return  # closed by us
                    if len(buf) == 0:  # Connection was closed.
                        raise ServerClosedError("Connection closed by peer")
                    unpacker.feed(buf)

            finally:
                hdl, self._handlers = self._handlers, None
                async with anyio.open_cancel_scope(shield=True):
                    for m in hdl.values():
                        try:
                            await m.cancel()
                        except anyio.exceptions.ClosedResourceError:
                            pass

    async def _request(self, action, iter=None, seq=None, _async=False, **params):
        """Send a request. Wait for a reply.

        Args:
          action (str): what to do. If ``seq`` is set, this is the stream's
            state, which should be ``None`` or ``'end'``.
          seq: Sequence number to use. Only when terminating a
            multi-message request.
          _async: don't wait for a reply (internal!)
          params: whatever other data the action needs
          iter: A flag how to treat multi-line replies.
            ``True``: always return an iterator
            ``False``: Never return an iterator, raise an error
                       if no or more than on reply arrives
            Default: ``None``: return a StreamedRequest if multi-line
                                otherwise return directly

        Any other keywords are forwarded to the server.
        """
        if self._handlers is None:
            raise anyio.ClosedResourceError()
        if seq is None:
            act = "action"
            self._seq += 1
            seq = self._seq
        else:
            act = "state"

        if action is not None:
            params[act] = action
        params["seq"] = seq
        res = _SingleReply(self, seq)
        self._handlers[seq] = res

        # logger.debug("Send %s", params)
        await self._send(**params)
        if _async:
            return res

        res = await res.get()
        if iter is True and not isinstance(res, StreamedRequest):

            async def send_one(res):
                yield res

            res = send_one(res)
        elif iter is False and isinstance(res, StreamedRequest):
            rr = None
            async for r in res:
                if rr is not None:
                    raise ManyData(action)
                rr = r
            if rr is None:
                raise NoData(action)
            res = rr
        return res

    @asynccontextmanager
    async def _stream(self, action, stream=False, **params):
        """Send and receive a multi-message request.

        Args:
          ``action``: what to do
          ``params``: whatever other data the action needs
          ``stream``: whether to enable multi-line requests
                      via ``await stream.send(**params)``

        This is a context manager. Use it like this::

            async with client._stream("update", path="private storage".split(),
                    stream=True) as req:
                with MsgReader("/tmp/msgs.pack") as f:
                    for msg in f:
                        await req.send(msg)
            # … or …
            async with client._stream("get_tree", path="private storage".split()) as req:
                for msg in req:
                    await process_entry(msg)
            # … or maybe … (auth does this)
            async with client._stream("interactive_thing", path=(None,"foo")) as req:
                msg = await req.recv()
                while msg.get(s,"")=="more":
                    await foo.send(s="more",value="some data")
                    msg = await req.recv()
                await foo.send(s="that's all then")

        Any server-side exception will be raised on recv.

        The server-side command will be killed if you leave the loop
        without having read a "state=end" message.
        """
        self._seq += 1
        seq = self._seq

        # logger.debug("Send %s", params)
        if self._handlers is None:
            raise anyio.exceptions.ClosedResourceError("Closed already")
        res = StreamedRequest(self, seq, stream=stream)
        await res.send(action=action, **params)
        try:
            yield res
        except BaseException as exc:
            if stream:
                await res.send(error=repr(exc))
            raise
        finally:
            await res.aclose()

    async def _run_auth(self, auth=None):
        hello = self._server_init
        sa = hello.get("auth", ())
        if not sa or not sa[0]:
            # no auth required
            if auth:
                logger.info(
                    "Tried to use auth=%s, but not required.", auth._auth_method
                )
            return
        if not auth:
            raise ClientAuthRequiredError("You need to log in using:", sa[0])
        if auth._auth_method != sa[0]:
            raise ClientAuthMethodError(
                "You cannot use '%s' auth" % (auth._auth_method), sa
            )
        if getattr(auth, "_DEBUG", False):
            auth._length = 16
        await auth.auth(self)

    @asynccontextmanager
    async def _connected(self, tg, init_timeout=5, auth=None):
        """
        This async context manager handles the actual TCP connection to
        the DistKV server.
        """
        hello = ValueEvent()
        self._handlers[0] = hello

        # logger.debug("Conn %s %s",self.host,self.port)
        async with AsyncExitStack() as ex:
            self.exit_stack = ex
            sock = await ex.enter_async_context(
                await anyio.connect_tcp(self.host, self.port, ssl_context=self.ssl)
            )

            if self.ssl:
                await sock.start_tls()
            # logger.debug("ConnDone %s %s",self.host,self.port)
            try:
                self.tg = tg
                self._socket = sock
                await self.tg.spawn(self._reader)
                async with anyio.fail_after(init_timeout):
                    self._server_init = await hello.get()
                    await self._run_auth(auth)
                yield self
            except socket.error as e:
                raise ServerConnectionError(self.host, self.port) from e
            else:
                # This is intentionally not in the error path
                # cancelling the nursey causes open_client() to
                # exit without a yield which triggers an async error,
                # which masks the exception
                pass
            finally:
                async with anyio.open_cancel_scope(shield=True):
                    await self.tg.cancel_scope.cancel()
                    if self._socket is not None:
                        async with self._send_lock:
                            await self._socket.close()
                        self._socket = None
                    self.tg = None

    # externally visible interface ##########################

    def get(self, *path, nchain=0):
        """
        Retrieve the data at a particular subtree position.

        Usage::
            res = await client.get("foo","bar")

        If you want to update this value, you should retrieve its change chain entry
        so that a competing update can be detected::

            res = await client.get("foo","bar", nchain=-1)
            res = await client.set("foo","bar", value=res.value+1, chain=res.chain)

        For lower overhead and set-directly-after-get change, nchain may be 1 or 2.
        """
        return self._request(action="get_value", path=path, iter=False, nchain=nchain)

    def set(self, *path, value=NotGiven, chain=NotGiven, prev=NotGiven, nchain=0):
        """
        Set or update a value.

        Usage::
            await client.set("foo","bar", value="baz", chain=None)

        Arguments:
            value: the value to set. Duh. ;-)
            chain: the previous value's change chain. Use ``None`` for new values.
            prev: the previous value. Discouraged; use ``chain`` instead.
            nchain: set to retrieve the node's chain tag, for further updates.
        """
        if value is NotGiven:
            raise RuntimeError("You need to supply a value, or call 'delete'")

        kw = {}
        if prev is not NotGiven:
            kw["prev"] = prev
        if chain is not NotGiven:
            kw["chain"] = chain

        return self._request(
            action="set_value", path=path, value=value, iter=False, nchain=nchain, **kw
        )

    def delete(self, *path, value=NotGiven, chain=NotGiven, prev=NotGiven, nchain=0):
        """
        Delete a node.

        Usage::
            await client.delete("foo","bar")

        Arguments:
            chain: the previous value's change chain.
            prev: the previous value. Discouraged; use ``chain`` instead.
            nchain: set to retrieve the node's chain, for setting a new value.
        """
        kw = {}
        if prev is not NotGiven:
            kw["prev"] = prev
        if chain is not NotGiven:
            kw["chain"] = chain

        return self._request(
            action="delete_value", path=path, iter=False, nchain=nchain, **kw
        )

    def get_tree(self, *path, **kw):
        """
        Retrieve a complete DistKV subtree.

        This call results in a stream of tree nodes. Storage of these nodes,
        if required, is up to the caller. Also, the server does not
        take a snapshot for you, thus the data may be inconsistent.

        Use :meth:`mirror` if you want this tree to be kept up-to-date or
        if you need a consistent snapshot.

        Args:
          nchain (int): Length of change chain to add to the results, for updating.
          min_depth (int): min level of nodes to retrieve.
          max_depth (int): max level of nodes to retrieve.

        """
        return self._request(action="get_tree", path=path, iter=True, **kw)

    def delete_tree(self, *path, nchain=0):
        """
        Delete a whole subtree.

        If you set ``nchain``, this call will return an async iterator over
        the deleted nodes; if not, the single return value only contains the
        number of deleted nodes.
        """
        return self._request(action="delete_tree", path=path, nchain=nchain)

    def stop(self, seq: int):
        """End this stream or request.

        Args:
            seq: the sequence number of the request in question.

        TODO: DistKV doesn't do per-command flow control yet, so you should
        call this method from a different task if you don't want to risk a
        deadlock.
        """
        return self._request(task=seq)

    def watch(self, *path, **kw):
        """
        Return an async iterator of changes to a subtree.

        Args:
          fetch (bool): if ``True``, also send the currect state. Be aware
            that this may overlap with processing changes: you may get
            updates before the current state is completely transmitted.
          nchain: add the nodes' change chains.
          min_depth (int): min level of nodes to retrieve.
          max_depth (int): max level of nodes to retrieve.

        The result should be passed through a :cls:`distkv.util.PathLongener`.

        If ``fetch`` is set, a ``state="uptodate"`` message will be sent
        as soon as sending the current state is completed.

        DistKV will not send stale data, so you may always replace a path's
        old cached state with the newly-arrived data.
        """
        return self._stream(action="watch", path=path, iter=True, **kw)

    def mirror(self, *path, root_type=ClientRoot, **kw):
        """An async context manager that affords an update-able mirror
        of part of a DistKV store.

        Arguments:
          root_type (type): The class to use for the root. Must be
            :cls:`ClientRoot` or a subclass.

        Returns: the root of this tree.

        Usage::
            async with distkv.open_client() as c:
                async with c.mirror("foo", "bar", need_wait=True) as foobar:
                    r = await c.set_value("foo", "bar", "baz", value="test")
                    await foobar.wait_chain(r.chain)
                    assert foobar['baz'].value == "test"
                pass
                # At this point you can still access the tree's data
                # via ``foobar``, but they will no longer be kept up-to-date.

        """
        root = root_type(self, *path, **kw)
        return root.run()

    def serf_mon(self, tag: str, raw: bool = False):
        """
        Return an async iterator of tunneled Serf messages. This receives
        all messages sent using :meth:`serf_send` with the same tag.

        Args:
            tag: the "user:" tag to monitor.
                 The first character may not be '+'.
                 Do not include Serf's "user:" prefix.
            raw: If ``True``, will not try to msgpack-decode incoming
                 messages.

        Returns: a dict.
            data: decoded data. Not present when ``raw`` is set or the
                  decoder raised an exception.
            raw: un-decoded data. Not present when '`raw`` is not set and
                 decoding succeeded.
            error: Error message. Not present when ``raw`` is set or
                   ``data`` is present.
        usage::
            async with client.serf_mon("test") as cl:
                async for msg in cl:
                    if 'error' in msg:
                        raise RuntimeError(msg.error)
                    await process_test(msg.data)
        """
        return self._stream(action="serfmon", type=tag, raw=raw)

    def serf_send(self, tag: str, data=None, raw: bytes = None):
        """
        Tunnel a user-tagged message through Serf. This sends the message
        to all active callers of :meth:`serf_mon` which use the same tag.

        Args:
            tag: the "user:" tag to send to.
                 The first character may not be '+'.
                 Do not include Serf's "user:" prefix.
            data: to-be-encoded data (anything ``msgpack`` can process).
            raw: raw binary data to send, mutually exclusive with ``data``.
        """
        if raw is None:
            return self._request(action="serfsend", type=tag, data=data)
        else:
            return self._request(action="serfsend", type=tag, raw=raw)
