# dist-kv client

import trio
import outcome
import msgpack
import socket
import weakref
import heapq
from async_generator import asynccontextmanager
from asyncserf.util import ValueEvent
from .util import attrdict, gen_ssl, num2byte, byte2num
from .exceptions import (
    ClientAuthMethodError,
    ClientAuthRequiredError,
    ServerClosedError,
    ServerConnectionError,
    ServerError,
    CancelledError,
)

# from trio_log import LogStream

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
    client = Client(host, port, ssl=ssl)
    async with trio.open_nursery() as tg:
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
        self.path = parent.path + (name,)
        self.value = None
        self.chain = None
        self._root = weakref.ref(parent.root)
        self.client = parent.client

    @property
    def root(self):
        return self._root()

    @property
    def subpath(self):
        """Return the path, starting with the root."""
        return self.path[len(self.root.path) :]  # noqa: E203

    def __getitem__(self, k):
        try:
            c = self._children[k]
        except KeyError:
            c = ClientEntry(self, k)
            self._children[k] = c
        return c

    def __in__(self, k):
        return k in self._children

    async def update(self, value):
        """Update this node's value.

        This is a coroutine.
        """
        await self.client._request(
            "set_value", chain=self.chain, path=self.path, value=value
        )
        self.value = value


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


class ClientRoot(ClientEntry):
    """A helper class that represents the root of a server entry list."""

    def __init__(self, client, *path, need_wait=False):
        self._children = dict()
        self.client = client
        self.path = path
        self.value = None
        self._need_wait = need_wait
        if need_wait:
            self._waiters = dict()
            self._seen = dict()

    @property
    def root(self):
        return self

    @asynccontextmanager
    async def run(self, task_status=trio.TASK_STATUS_IGNORED):
        """A coroutine that fetches, and continually updates, a subtree.
        """
        async with trio.open_nursery() as n:
            self._nursery = n

            async def monitor(task_status=trio.TASK_STATUS_IGNORED):
                async with self.client._stream("watch", nchain=3, path=self.path) as w:
                    task_status.started()
                    async for r in w:
                        entry = self
                        for p in r.path:
                            entry = entry[p]
                        try:
                            assert _node_gt(r.chain, entry.chain)
                        except AttributeError:
                            pass
                        entry.value = r.value
                        entry.chain = r.chain

                        if not self._need_wait:
                            return
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
                                while w[0][0] <= c.node:
                                    heapq.heappop(w)[1].set()
                            c = c.get("prev", None)

            await n.start(monitor)
            async with self.client._stream("get_tree", nchain=3, path=self.path) as w:
                async for r in w:
                    entry = self
                    for p in r.path:
                        entry = entry[p]
                    if entry.value is None:
                        entry.value = r.value
                        entry.chain = r.chain

            task_status.started()
            try:
                yield self
            finally:
                n.cancel_scope.cancel()

    async def cancel(self):
        """Stop the monitor"""
        self._nursery.cancel_scope.cancel()

    async def wait_chain(self, chain):
        """Wait for a tree update containing this tick."""
        try:
            if chain.tick <= self._seen[chain.node]:
                return
        except KeyError:
            pass
        w = self._waiters.setdefault(chain.node, [])
        e = trio.Event()
        heapq.heappush(w, (chain.tick, e))
        await w.wait()


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
        self.send_q, self.recv_q = trio.open_memory_channel(100)
        self._client._handlers[seq] = self
        self._reply_stream = None
        self.n_msg = 0
        self._report_start = report_start
        # None: no message yet; True: begin seen; False: end or single message seen

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        self.n_msg += 1
        if "error" in msg:
            try:
                await self.send_q.send(outcome.Error(ServerError(msg.error)))
            except trio.ClosedResourceError:
                pass
            return
        state = msg.get("state", "")

        if state == "start":
            if self._reply_stream is not None:
                raise RuntimeError("Recv state 2", self._reply_stream, msg)
            self._reply_stream = True
            self.start_msg = msg
            if self._report_start:
                await self.send_q.send(outcome.Value(msg))

        elif state == "end":
            if self._reply_stream is not True:
                raise RuntimeError("Recv state 3", self._reply_stream, msg)
            self._reply_stream = None
            self.end_msg = msg
            await self.send_q.aclose()
            return False

        else:
            if state not in ("", "uptodate"):
                logger.warning("Unknown state: %s", msg)

            if self._reply_stream is False:
                raise RuntimeError("Recv state 1", self._reply_stream, msg)
            elif self._reply_stream is None:
                self._reply_stream = False
            await self.send_q.send(outcome.Value(msg))
            if self._reply_stream is False:
                await self.send_q.aclose()

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
        try:
            res = await self.recv_q.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration
        return res.unwrap()

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
        await self.send_q.send(outcome.Error(CancelledError()))
        await self.aclose()

    async def aclose(self, timeout=0.2):
        await self.send_q.aclose()
        if self._stream == 2:
            await self._client._send(seq=self.seq, state="end")
            if timeout is not None:
                with trio.move_on_after(timeout):
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

    def __init__(self, host, port, ssl=False):
        self.host = host
        self.port = port
        self._seq = 0
        self._handlers = {}
        self._send_lock = trio.Lock()
        self.ssl = gen_ssl(ssl, server=False)

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

            k = await trio.run_sync_in_worker_thread(gen_key)
            res = await self._request(
                "diffie_hellman", pubkey=num2byte(k.public_key), length=length
            )  # length=k.key_length
            await trio.run_sync_in_worker_thread(
                k.generate_shared_secret, byte2num(res.pubkey)
            )
            self._dh_key = num2byte(k.shared_secret)[0:32]
        return self._dh_key

    async def _send(self, **params):
        async with self._send_lock:
            await self._socket.send_all(_packer(params))

    async def _reader(self, *, task_status=trio.TASK_STATUS_IGNORED):
        """Main loop for reading
        """
        unpacker = msgpack.Unpacker(
            object_pairs_hook=attrdict, raw=False, use_list=False
        )

        with trio.CancelScope(shield=True) as s:
            task_status.started(s)
            try:
                while True:
                    for msg in unpacker:
                        # logger.debug("Recv %s", msg)
                        try:
                            await self._handle_msg(msg)
                        except trio.ClosedResourceError:
                            raise RuntimeError(msg)

                    if self._socket is None:
                        break
                    try:
                        buf = await self._socket.receive_some(4096)
                    except trio.ClosedResourceError:
                        return  # closed by us
                    if len(buf) == 0:  # Connection was closed.
                        raise ServerClosedError("Connection closed by peer")
                    unpacker.feed(buf)

            finally:
                hdl, self._handlers = self._handlers, None
                with trio.CancelScope(shield=True):
                    try:
                        for m in hdl.values():
                            await m.cancel()
                    except trio.ClosedResourceError:
                        pass

    async def _request(self, action, iter=None, seq=None, _async=False, **params):
        """Send a request. Wait for a reply.

        Args:
          ``action``: what to do. If ``seq`` is set, this is the stream's
                      state, which should be ``None`` or ``'end'``.
          ``seq``: Sequence number to use. Only when terminating a
                   multi-message request.
          ``_async``: don't wait for a reply (internal!)
          ``params``: whatever other data the action needs
          ``iter``: A flag how to treat multi-line replies.
                    ``True``: always return an iterator
                    ``False``: Never return an iterator, raise an error
                               if no or more than on reply arrives
                    Default: ``None``: return a StreamedRequest if multi-line
                                       otherwise return directly
        """
        if self._handlers is None:
            raise trio.ClosedResourceError()
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
        async with await trio.open_tcp_stream(self.host, self.port) as sock:
            if self.ssl:
                # sock = LogStream(sock,"CL")
                sock = trio.SSLStream(sock, self.ssl, server_side=False)
                # sock = LogStream(sock,"CH")
                await sock.do_handshake()
            # logger.debug("ConnDone %s %s",self.host,self.port)
            try:
                self.tg = tg
                self._socket = sock
                await self.tg.start(self._reader)
                with trio.fail_after(init_timeout):
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
                self.tg.cancel_scope.cancel()
            finally:
                if self._socket is not None:
                    await self._socket.aclose()
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

    def set(self, *path, value=NoData, chain=NoData, prev=NoData, nchain=0):
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
        if value is NoData:
            raise RuntimeError("You need to supply a value")

        kw = {}
        if prev is not NoData:
            kw["prev"] = prev
        if chain is not NoData:
            kw["chain"] = chain

        return self._request(
            action="set_value", path=path, value=value, iter=False, nchain=nchain, **kw
        )

    def delete(self, *path, value=NoData, chain=NoData, prev=NoData, nchain=0):
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
        if prev is not NoData:
            kw["prev"] = prev
        if chain is not NoData:
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
            nchain: Length of change chain to add to the results, for updating.
            min_depth, max_depth: level of nodes to retrieve.

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

    def watch(self, *path, fetch=False, nchain=0):
        """
        Return an async iterator of changes to a subtree.

        Args:
            fetch: if ``True``, also send the currect state. Be aware that
                   this may overlap with processing changes: you may get
                   updates before the current state is completely
                   transmitted.
            nchain: add the nodes' change chains.

        The result should be passed through a :cls:`distkv.util.PathLongener`.

        If ``fetch`` is set, a ``state="uptodate"`` message will be sent
        as soon as sending the current state is completed.

        DistKV will not send stale data, so you may always replace a path's
        old cached state with the newly-arrived data.
        """
        return self._stream(
            action="watch", path=path, iter=True, nchain=nchain, fetch=fetch
        )

    def mirror(self, *path, **kw):
        """An async context manager that affords an update-able mirror
        of part of a DistKV store.

        Returns: the root of this tree.

        Usage::
            async with distkv.open_client() as c:
                async with c.mirror("foo", "bar", need_wait=True) as foobar:
                    r = await c.set_value("foo", "bar", "baz", value="test")
                    foobar.wait_chain(r.chain)
                    assert foobar['baz'].value == "test"
                pass
                # At this point you can still access the tree's data
                # via ``foobar``, but they will no longer be kept up-to-date.

        """
        root = ClientRoot(self, *path, **kw)
        return root.run()

    def serf_mon(self, tag: str, raw: bool = False):
        """
        Return an async iterator of tunneled Serf messages. This receives
        all messages sent using :meth_`serf_send` using the same tag.

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
        """
        return self._stream(action="serfmon", type=tag, raw=raw)

    def serf_send(self, tag: str, data = None, raw: bytes = None):
        """
        Tunnel a user-tagged message through Serf. This sends the message
        to all active callers of :meth:`serf_send` using the same tag.

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

