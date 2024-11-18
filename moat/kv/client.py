"""
Client code.

Main entry point: :func:`open_client`.
"""

import logging
import os
import socket
from contextlib import AsyncExitStack, asynccontextmanager
from inspect import iscoroutine
from pathlib import Path
from typing import Tuple

import anyio
from asyncscope import Scope, main_scope, scope
from moat.util import (  # pylint: disable=no-name-in-module
    DelayedRead,
    DelayedWrite,
    NotGiven,
    OptCtx,
    PathLongener,
    ValueEvent,
    attrdict,
    byte2num,
    combine_dict,
    create_queue,
    gen_ssl,
    num2byte,
    yload,
)

from .codec import packer, stream_unpacker
from .exceptions import (
    CancelledError,
    ClientAuthMethodError,
    ClientAuthRequiredError,
    ServerClosedError,
    ServerConnectionError,
    ServerError,
    error_types,
)

logger = logging.getLogger(__name__)

ClosedResourceError = anyio.ClosedResourceError

rs = os.environ.get("PYTHONHASHSEED", None)
if rs is None:
    import random
else:  # pragma: no cover
    try:
        import trio._core._run as tcr
    except ImportError:
        import random
    else:
        random = tcr._r

__all__ = ["NoData", "ManyData", "open_client", "client_scope", "StreamedRequest"]


class AsyncValueEvent(ValueEvent):
    def cancel(self):
        if self.scope is not None:
            self.scope.cancel()
        super().set_error(CancelledError())


class NoData(ValueError):
    """No reply arrived"""


class ManyData(ValueError):
    """More than one reply arrived"""


@asynccontextmanager
async def open_client(_main_name="moat.kv.client", **cfg):
    """
    This async context manager returns an opened client connection.

    There is no attempt to reconnect if the connection should fail.
    """

    async with OptCtx(main_scope(name=_main_name) if scope.get() is None else None):
        yield await client_scope(**cfg)


async def _scoped_client(_name=None, **cfg):
    """
    AsyncScope service for a client connection.
    """
    client = Client(cfg)
    async with client._connected() as client:
        scope.register(client)
        await scope.wait_no_users()


_cid = 0


async def client_scope(_name=None, **cfg):
    """
    Returns an opened client connection, by way of an asyncscope service.

    The configuration's 'connect' dict may include a name to disambiguate
    multiple connections.

    There is no attempt to reconnect if the connection should fail.
    """

    if _name is None:
        _name = cfg.get("conn", {}).get("name", "conn")
        if _name is None:
            global _cid
            _cid += 1
            _name = f"_{_cid}"
            # uniqueness required for testing.
            # TODO replace with a dependency on the test server.
    return await scope.service(
        f"moat.kv.client.{_name}", _scoped_client, _name=_name, **cfg
    )


class StreamedRequest:
    """
    This class represents a bidirectional multi-message request.

    stream: True if you need to send a multi-message request.
            Set to None if you already sent a single-message request.
    report_start: True if the initial state=start message of a multi-reply
                  should be included in the iterator.
                  If False, the message is available as ``.start_msg``.
    TODO: add rate limit.

    Call ``.send(**params)`` to send something; call ``.recv()``
    or async-iterate for receiving.
    """

    start_msg = None
    end_msg = None
    qr = None
    dw = None

    def __init__(self, client, seq, stream: bool = False, report_start: bool = False):
        self._stream = stream
        self._client = client
        self.seq = seq
        self._open = False
        self._client._handlers[seq] = self
        self._reply_stream = None
        self.n_msg = 0
        self._report_start = report_start
        self._started = anyio.Event()
        self._path_long = lambda x: x
        if client.qlen > 0:
            self.dw = DelayedWrite(client.qlen)
            self.qr = DelayedRead(
                client.qlen, get_seq=self._get_seq, send_ack=self._send_ack
            )
        else:
            self.qr = create_queue(client.config.server.buffer)

    @staticmethod
    def _get_seq(msg):
        return msg.pop("wseq", 0)

    async def _send_ack(self, seq):
        msg = dict(seq=self.seq, state="ack", ack=seq)
        self._client.logger.debug("Send %s", msg)
        await self._client._send(**msg)

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        self.n_msg += 1
        if "error" in msg:
            logger.info("ErrorMsg: %s", msg)
            try:
                cls = error_types[msg["etype"]]
            except KeyError:
                cls = ServerError
            try:
                await self.qr.put_error(cls(msg.error))
            except anyio.BrokenResourceError:
                raise cls(msg.error)
            return
        self._client.logger.debug("Reply %s", msg)
        state = msg.get("state", "")

        if state == "start":
            if self._reply_stream is not None:  # pragma: no cover
                raise RuntimeError("Recv state 2", self._reply_stream, msg)
            self._reply_stream = True
            self.start_msg = msg
            self._started.set()
            if self._report_start:
                await self.qr.put(msg)

        elif state == "end":
            if self._reply_stream is not True:  # pragma: no cover
                raise RuntimeError("Recv state 3", self._reply_stream, msg)
            self._reply_stream = None
            self.end_msg = msg
            if self.qr is not None:
                self.qr.close_sender()
            return False

        elif state == "ack":
            if self.dw is not None:
                await self.dw.recv_ack(msg["ack"])

        else:
            if state not in ("", "uptodate"):  # pragma: no cover
                logger.warning("Unknown state: %s", msg)

            if self._reply_stream is False:  # pragma: no cover
                raise RuntimeError("Recv state 1", self._reply_stream, msg)
            elif self._reply_stream is None:
                self._reply_stream = False
            try:
                with anyio.fail_after(1):
                    await self.qr.put(msg)
            except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                logger.warning("Reader for %s closed: %s", self.seq, msg)
            if self._reply_stream is False:
                self.qr.close_sender()

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
            res = await self.qr.get()
        except (anyio.EndOfStream, anyio.ClosedResourceError, EOFError):
            raise StopAsyncIteration
        except CancelledError:
            raise StopAsyncIteration  # just terminate
        self._path_long(res)
        return res

    async def send(self, **msg):
        # self._client.logger.debug("Send %s", msg)
        if not self._open:
            if self._stream:
                msg["state"] = "start"
            self._open = True
        elif msg.get("state", "") == "end":
            self._open = False

        if self.dw is not None:
            msg["wseq"] = await self.dw.next_seq()
        msg["seq"] = self.seq
        self._client.logger.debug("Send %s", msg)
        await self._client._send(**msg)

    async def recv(self):
        return await self.__anext__()

    async def cancel(self):
        try:
            await self.qr.put_error(CancelledError())
        except (anyio.BrokenResourceError, anyio.ClosedResourceError, EOFError):
            pass
        else:
            try:
                await self.aclose()
            except ServerClosedError:
                pass

    async def wait_started(self):
        await self._started.wait()

    async def aclose(self):
        try:
            if self._stream:
                msg = dict(seq=self.seq, state="end")
                # self._client.logger.debug("SendE %s", msg)
                await self._client._send(**msg)
            if self._open:
                msg = dict(action="stop", task=self.seq)
                # self._client.logger.debug("SendC %s", msg)
                try:
                    await self._client._request(**msg, _async=True)
                except (ServerClosedError, anyio.BrokenResourceError):
                    pass
                # ignore the reply
        finally:
            self.qr.close_sender()
            self.qr.close_receiver()


class _SingleReply:
    """
    This class represents a single-message reply.
    It will delegate itself to a StreamedRequest if a multi message reply
    arrives.
    """

    def __init__(self, conn, seq, params):
        self._conn = conn
        self.seq = seq
        self.q = AsyncValueEvent()
        self._params = params

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        if msg.get("state") == "start":
            res = StreamedRequest(self._conn, self.seq, stream=None)
            await res.set(msg)
            self.q.set(res)
            return res
        elif "error" in msg:
            msg["request_params"] = self._params
            logger.info("ErrorMsg: %s", msg)
            self.q.set_error(ServerError(msg.error))
        else:
            self.q.set(msg)
        return False

    async def get(self):
        """Wait for and return the result.

        This is a coroutine.
        """
        return await self.q.get()

    async def cancel(self):
        pass


class ClientConfig:
    """Accessor for configuration, possibly stored in MoaT-KV."""

    _changed = None  # pylint

    def __init__(self, client, *a, **k):  # pylint: disable=unused-argument
        self._init(client)

    def _init(self, client):
        self._client = client
        self._current = {}
        self._changed = anyio.Event()

    def __getattr__(self, k):
        if k.startswith("_"):
            return object.__getattribute__(self, k)
        v = self._current.get(k, NotGiven)
        if v is NotGiven:
            try:
                v = self._client._cfg[k]
            except KeyError:
                raise AttributeError(k) from None
        return v

    def __contains__(self, k):
        return k in self._current or k in self._client._cfg

    async def _update(self, k, v):
        """
        Update this config entry. The new data is combined with the static
        configuration; the old data is discarded.
        """
        self._current[k] = combine_dict(v, self._client._cfg.get(k, {}))
        c, self._changed = self._changed, anyio.Event()
        c.set()

    async def _watch(self):
        class CfgWatcher:
            def __ainit__(slf):  # pylint: disable=no-self-argument
                return slf

            async def __anext__(slf):  # pylint: disable=no-self-argument
                await self._changed.wait()

        return CfgWatcher()


class Client:
    """
    The client side of a MoaT-KV connection.

    Use `open_client` or `client_scope` to use this class.
    """

    _server_init = None  # Server greeting
    _dh_key = None
    _config = None
    _socket = None
    tg: anyio.abc.TaskGroup = None
    scope: Scope = None
    _n = None
    exit_stack = None

    server_name = None
    client_name = None
    qlen: int = 0

    def __init__(self, cfg: dict):
        CFG = yload(Path(__file__).parent / "_config.yaml")
        self._cfg = combine_dict(cfg, CFG["kv"], cls=attrdict)
        self.config = ClientConfig(self)

        self._seq = 0
        self._handlers = {}
        self._send_lock = anyio.Lock()
        self._helpers = {}
        self._name = "".join(random.choices("abcdefghjkmnopqrstuvwxyz23456789", k=9))
        self.logger = logging.getLogger(f"moat.kv.client.{self._name}")

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

    async def unique_helper(self, name, factory):
        """
        Run a (single) async context manager as a service.
        """

        async def with_factory(f):
            async with f() as r:
                scope.register(r)
                await scope.no_more_dependents()

        return await scope.service(name, with_factory, factory)

    async def _handle_msg(self, msg):
        try:
            seq = msg.seq
        except AttributeError:
            if "error" in msg:
                raise RuntimeError("Server error", msg.error) from None
            raise RuntimeError("Reader got out of sync: " + str(msg)) from None
        try:
            hdl = self._handlers[seq]
        except KeyError:
            logger.warning("Spurious message %s: %s", seq, msg)
            return

        res = hdl.set(msg)
        if iscoroutine(res):
            res = await res
        elif res.__class__.__name__ == "DeprecatedAwaitable":
            res = None
        if res is False:
            del self._handlers[seq]
        elif res:
            self._handlers[seq] = res

    async def dh_secret(self, length=1024):
        """Exchange a diffie-hellman secret with the server"""
        if self._dh_key is None:
            from moat.lib.diffiehellman import DiffieHellman

            def gen_key():
                k = DiffieHellman(key_length=length, group=(5 if length < 32 else 14))
                k.generate_public_key()
                return k

            k = await anyio.to_thread.run_sync(gen_key)
            res = await self._request(
                "diffie_hellman", pubkey=num2byte(k.public_key), length=length
            )  # length=k.key_length
            await anyio.to_thread.run_sync(
                k.generate_shared_secret, byte2num(res.pubkey)
            )
            self._dh_key = num2byte(k.shared_secret)[0:32]
        return self._dh_key

    async def _send(self, **params):
        async with self._send_lock:
            sock = self._socket
            if sock is None:
                raise ServerClosedError("Disconnected")

            try:
                p = packer(params)
            except TypeError as e:
                raise ValueError(f"Unable to pack: {params!r}") from e
            await sock.send(p)

    async def _reader(self, *, evt=None):
        """Main loop for reading"""
        unpacker = stream_unpacker()

        with anyio.CancelScope():
            # XXX store the scope so that the redaer may get cancelled?
            if evt is not None:
                await evt.set()
            try:
                while True:
                    for msg in unpacker:
                        # self.logger.debug("Recv %s", msg)
                        try:
                            await self._handle_msg(msg)
                        except ClosedResourceError as exc:
                            logger.warning("Reader closed in handler", exc_info=exc)
                            return

                    if self._socket is None:
                        logger.warning("Reader socket closed")
                        break
                    try:
                        buf = await self._socket.receive(4096)
                    except anyio.EndOfStream:
                        raise ServerClosedError("Connection closed by peer")
                    except ClosedResourceError:
                        return  # closed by us
                    if len(buf) == 0:  # Connection was closed.
                        raise ServerClosedError("Connection closed by peer")
                    unpacker.feed(buf)

            except BaseException as exc:
                logger.warning("Reader died: %r", exc, exc_info=exc)
                raise
            finally:
                with anyio.fail_after(2, shield=True):
                    hdl, self._handlers = self._handlers, None
                    for m in hdl.values():
                        try:
                            res = m.cancel()
                            if iscoroutine(res):
                                await res
                        except ClosedResourceError:
                            pass

    async def _request(self, action, iter=None, seq=None, _async=False, **params):  # pylint: disable=redefined-builtin  # iter
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
            raise ClosedResourceError()
        if seq is None:
            act = "action"
            self._seq += 1
            seq = self._seq
        else:
            act = "state"

        if action is not None:
            params[act] = action
        params["seq"] = seq
        res = _SingleReply(self, seq, params)
        self._handlers[seq] = res

        self.logger.debug("Send %s", params)
        await self._send(**params)
        if _async:
            return res

        res = await res.get()
        if isinstance(res, dict):
            self.logger.debug("Result %s", res)

        if iter is True and not isinstance(res, StreamedRequest):

            async def send_one(res):
                yield res

            res = send_one(res)

        elif iter is False and isinstance(res, StreamedRequest):
            rr = None
            try:
                async for r in res:
                    if rr is not None:
                        raise ManyData(action)
                    rr = r
            finally:
                await res.aclose()
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

            async with client._stream("update", path=P("private.storage"),
                    stream=True) as req:
                with MsgReader("/tmp/msgs.pack") as f:
                    for msg in f:
                        await req.send(msg)
            # … or …
            async with client._stream("get_tree", path=P("private.storage)) as req:
                for msg in req:
                    await process_entry(msg)
            # … or maybe … (auth does this)
            async with client._stream("interactive_thing", path=P(':n.foo)) as req:
                msg = await req.recv()
                while msg.get(s,"") == "more":
                    await foo.send(s="more",value="some data")
                    msg = await req.recv()
                await foo.send(s="that's all then")

        Any server-side exception will be raised on recv.

        The server-side command will be killed if you leave the loop
        without having read a "state=end" message.
        """
        self._seq += 1
        seq = self._seq

        # self.logger.debug("Send %s", params)
        if self._handlers is None:
            raise ClosedResourceError("Closed already")
        res = StreamedRequest(self, seq, stream=stream)
        if "path" in params and params.get("long_path", False):
            res._path_long = PathLongener(params["path"])
        await res.send(action=action, **params)
        await res.wait_started()
        try:
            yield res
        except BaseException as exc:
            if stream:
                try:
                    await res.send(error=repr(exc))
                except anyio.ClosedResourceError:
                    pass
            raise
        finally:
            with anyio.fail_after(2, shield=True):
                try:
                    await res.aclose()
                except anyio.ClosedResourceError:
                    pass

    async def _run_auth(self, auth=None):
        """
        As the name implies: process authorization.
        """
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
                f"You cannot use {auth._auth_method!r} auth", sa
            )
        if getattr(auth, "_DEBUG", False):
            auth._length = 16
        await auth.auth(self)

    @asynccontextmanager
    async def _connected(self):
        """
        This async context manager handles the actual TCP connection to
        the MoaT-KV server.
        """
        hello = AsyncValueEvent()
        self._handlers[0] = hello

        cfg = self._cfg["conn"]
        host = cfg["host"]
        port = cfg["port"]
        auth = cfg["auth"]
        if auth is not None:
            from .auth import gen_auth

            auth = gen_auth(auth)
        init_timeout = cfg["init_timeout"]
        ssl = gen_ssl(cfg["ssl"], server=False)

        # self.logger.debug("Conn %s %s",self.host,self.port)
        try:
            ctx = await anyio.connect_tcp(host, port)
        except socket.gaierror:
            raise ServerConnectionError(host, port)
        if ssl:
            raise NotImplementedError("XXX TODO fix SSL")
            # ctx = await anyio.streams.tls.TLSStream(ctx, ssl_context=ssl, server_side=False)
        try:
            async with ctx as stream, AsyncExitStack() as ex:
                self.scope = scope.get()
                # self.tg = tg  # TODO might not be necessary
                self.exit_stack = ex

                try:
                    self._socket = stream
                    await self.scope.spawn(self._reader)
                    with anyio.fail_after(init_timeout):
                        self._server_init = msg = await hello.get()
                        self.logger.debug("Hello %s", msg)
                        self.server_name = msg.node
                        self.client_name = (
                            cfg["name"] or socket.gethostname() or self.server_name
                        )
                        if "qlen" in msg:
                            self.qlen = min(msg.qlen, 99)  # self.config.server.buffer
                            await self._send(seq=0, qlen=self.qlen)
                        await self._run_auth(auth)

                    from .config import ConfigRoot

                    self._config = await ConfigRoot.as_handler(
                        self, require_client=False
                    )

                except TimeoutError:
                    raise
                except socket.error as e:
                    raise ServerConnectionError(host, port) from e
                else:
                    yield self
                finally:
                    # Clean up our hacked config
                    try:
                        del self._config
                    except AttributeError:
                        pass
                    self.config = ClientConfig(self)
        finally:
            self._socket = None
            self.tg = None

    # externally visible interface ##########################

    def get(self, path, *, nchain=0):
        """
        Retrieve the data at a particular subtree position.

        Usage::
            res = await client.get(P("foo.bar"))

        If you want to update this value, you should retrieve its change chain entry
        so that a competing update can be detected::

            res = await client.get("foo","bar", nchain=-1)
            res = await client.set("foo","bar", value=res.value+1, chain=res.chain)

        For lower overhead and set-directly-after-get change, nchain may be 1 or 2.

        Arguments:
            path (Path): the path to update.
            nchain: set to retrieve the node's chain tag, for later updates.
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        return self._request(action="get_value", path=path, iter=False, nchain=nchain)

    def set(
        self,
        path,
        value=NotGiven,
        *,
        chain=NotGiven,
        prev=NotGiven,
        nchain=0,
        idem=None,
    ):
        """
        Set or update a value.

        Usage::
            await client.set(P("foo.bar"), value="baz", chain=None)

        Arguments:
            path (Path): the path to update.
            value: the value to set. Duh. ;-)
            chain: the previous value's change chain. Use ``None`` for new values.
            prev: the previous value. Discouraged; use ``chain`` instead.
            nchain: set to retrieve the node's chain tag, for further updates.
            idem: if True, no-op if the value doesn't change
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        if value is NotGiven:
            raise RuntimeError("You need to supply a value, or call 'delete'")

        kw = {}
        if prev is not NotGiven:
            kw["prev"] = prev
        if chain is not NotGiven:
            kw["chain"] = chain
        if idem is not None:
            kw["idem"] = idem

        return self._request(
            action="set_value", path=path, value=value, iter=False, nchain=nchain, **kw
        )

    def delete(self, path, *, chain=NotGiven, prev=NotGiven, nchain=0, recursive=False):
        """
        Delete a node.

        Usage::
            await client.delete(P("foo.bar"))

        Arguments:
            path (Path): the path of the entry to remove.
            chain: the previous value's change chain.
            prev: the previous value. Discouraged; use ``chain`` instead.
            nchain: set to retrieve the node's chain, for setting a new value.
            recursive: delete the whole subtree. Cannot be used with
                ``chain`` and/or ``prev``.
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        kw = {}
        if prev is not NotGiven:
            kw["prev"] = prev
        if chain is not NotGiven:
            kw["chain"] = chain

        return self._request(
            action="delete_tree" if recursive else "delete_value",
            path=path,
            iter=False,
            nchain=nchain,
            **kw,
        )

    async def list(self, path, *, with_data=False, empty=None, **kw):
        """
        Retrieve the next data level.

        Args:
          path (Path): the path to retrieve the entries from.
          with_data (bool): Return the data along with the keys. Default False.
          empty (bool): Return [names of] empty nodes. Default True if
            with_data is not set.
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        if empty is None:
            empty = not with_data
        res = await self._request(
            action="enum", path=path, with_data=with_data, empty=empty, **kw
        )
        try:
            return res.result
        except AttributeError:
            raise res.q.value.error from None  # XXX fix this

    async def get_tree(self, path, *, long_path=True, **kw):
        """
        Retrieve a complete MoaT-KV subtree.

        This call results in a stream of tree nodes. Storage of these nodes,
        if required, is up to the caller. Also, the server does not
        take a snapshot for you, thus the data may be inconsistent.

        Use :meth:`mirror` if you want this tree to be kept up-to-date or
        if you need a consistent snapshot.

        Args:
          path (Path): the path to retrieve the entries from.
          nchain (int): Length of change chain to add to the results, for updating.
          min_depth (int): min level of nodes to retrieve.
          max_depth (int): max level of nodes to retrieve.
          long_path (bool): if set (the default), pass the result through PathLongener

        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        if long_path:
            lp = PathLongener()
        async for r in await self._request(
            action="get_tree", path=path, iter=True, long_path=True, **kw
        ):
            if long_path:
                lp(r)
            yield r

    def delete_tree(self, path, *, nchain=0):
        """
        Delete a whole subtree.

        If you set ``nchain``, this call will return an async iterator over
        the deleted nodes; if not, the single return value only contains the
        number of deleted nodes.
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        return self._request(action="delete_tree", path=path, nchain=nchain)

    def stop(self, seq: int):
        """End this stream or request.

        Args:
            seq: the sequence number of the request in question.

        TODO: MoaT-KV doesn't do per-command flow control yet, so you should
        call this method from a different task if you don't want to risk a
        deadlock.
        """
        return self._request(action="stop", task=seq)

    def watch(self, path, *, long_path=True, **kw):
        """
        Return an async iterator of changes to a subtree.

        Args:
          path (Path): the path to monitor entries at.
          fetch (bool): if ``True``, also send the currect state. Be aware
            that this may overlap with processing changes: you may get
            updates before the current state is completely transmitted.
          nchain: add the nodes' change chains.
          min_depth (int): min level of nodes to retrieve.
          max_depth (int): max level of nodes to retrieve.

        The result should be passed through a :class:`moat.kv.util.PathLongener`.

        If ``fetch`` is set, a ``state="uptodate"`` message will be sent
        as soon as sending the current state is completed.

        MoaT-KV will not send stale data, so you may always replace a path's
        old cached state with the newly-arrived data.
        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        return self._stream(
            action="watch", path=path, iter=True, long_path=long_path, **kw
        )

    def mirror(self, path, *, root_type=None, **kw):
        """An async context manager that affords an update-able mirror
        of part of a MoaT-KV store.

        Arguments:
          root_type (type): The class to use for the root. Must be
            :class:`MirrorRoot` or a :class:`ClientRoot` subclass.

        Returns: the root of this tree.

        Usage::
            async with moat.kv.open_client() as c:
                async with c.mirror("foo", "bar", need_wait=True) as foobar:
                    r = await c.set_value("foo", "bar", "baz", value="test")
                    await foobar.wait_chain(r.chain)
                    assert foobar['baz'].value == "test"
                pass
                # At this point you can still access the tree's data
                # via ``foobar``, but they will no longer be kept up-to-date.

        """
        if isinstance(path, str):
            raise RuntimeError("You need a path, not a string")
        if root_type is None:
            from .obj import MirrorRoot

            root_type = MirrorRoot
        root = root_type(self, path, **kw)
        return root.run()

    def msg_monitor(self, topic: Tuple[str], raw: bool = False):
        """
        Return an async iterator of tunneled messages. This receives
        all messages sent using :meth:`msg_send` with the same topic.

        Args:
            topic: the topic to monitor.
            raw: If ``True``, will not try to msgpack-decode incoming messages.

        Returns: an iterator yielding a dict.
            topic: actual topic the message was received at.
            data: decoded data. Not present when ``raw`` is set or the
                  decoder raised an exception.
            raw: un-decoded data. Not present when '`raw`` is not set and
                 decoding succeeded.
            error: Error message. Not present when ``raw`` is set or
                   ``data`` is present.
        usage::
            async with client.msg_monitor(["test"]) as cl:
                async for msg in cl:
                    if 'error' in msg:
                        raise RuntimeError(msg.error)
                    await process_test(msg.data)
        """
        return self._stream(action="msg_monitor", topic=topic, raw=raw)

    def msg_send(self, topic: Tuple[str], data=None, raw: bytes = None):
        """
        Tunnel a user-tagged message. This sends the message
        to all active callers of :meth:`msg_monitor` which use the same topic.

        Args:
            topic: the MQTT topic to send to.
            data: to-be-encoded data (anything ``msgpack`` can process).
            raw: raw binary data to send, mutually exclusive with ``data``.
        """
        if raw is None:
            return self._request(action="msg_send", topic=topic, data=data)
        elif data is not None and data is not NotGiven:
            raise RuntimeError("You can either send raw or non-raw data")
        else:
            return self._request(action="msg_send", topic=topic, raw=raw)
