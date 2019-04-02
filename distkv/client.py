# dist-kv client

import trio
import outcome
import msgpack
import socket
from async_generator import asynccontextmanager
from trio_serf.util import ValueEvent
from .util import attrdict, Queue, AsyncValueEvent
from .exceptions import ClientAuthMethodError, ClientAuthRequiredError, ServerClosedError,ServerConnectionError,ServerError
from concurrent.futures import CancelledError

import logging
logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

__all__ = ['NodData', 'ManyData', 'open_client', 'StreamedRequest']

class NoData(ValueError):
    """No reply arrived"""

class ManyData(ValueError):
    """More than one reply arrived"""

@asynccontextmanager
async def open_client(host, port, init_timeout=5, auth=None):
    client = Client(host, port)
    async with trio.open_nursery() as tg:
        async with client._connected(tg, init_timeout=init_timeout, auth=auth) as client:
            yield client


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
    send_stop = True
    start_msg = None
    end_msg = None

    def __init__(self, client, seq, stream:bool=False, report_start:bool=False):
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
        if 'error' in msg:
            try:
                await self.send_q.send(outcome.Error(ServerError(msg.error)))
            except trio.ClosedResourceError:
                pass
            return
        state = msg.get('state', "")
        if state == "":
            if self._reply_stream is False:
                raise RuntimeError("Recv state 1", self._reply_stream, msg)
            elif self._reply_stream is None:
                self._reply_stream = False
            await self.send_q.send(outcome.Value(msg))
            if self._reply_stream is False:
                await self.send_q.aclose()

        elif state == 'start':
            if self._reply_stream is not None:
                raise RuntimeError("Recv state 2", self._reply_stream, msg)
            self._reply_stream = True
            self.start_msg = msg
            if self._report_start:
                await self.send_q.send(outcome.Value(msg))

        elif state == 'end':
            if self._reply_stream is not True:
                raise RuntimeError("Recv state 3", self._reply_stream, msg)
            self._reply_stream = None
            self.end_msg = msg
            await self.send_q.aclose()
            return False

        else:
            logger.warning("Unknown state: %s", msg)

    async def get(self):
        """Receive a single reply"""
        pass # receive reply
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
        #logger.debug("Send %s", params)
        if not self._stream:
            if self._stream is None:
                raise RuntimeError("You can't send more than one request")
            self._stream = None
        elif self._stream is True:
            self._stream = 2
            params['state'] = 'start'
        elif self._stream == 2 and params.get('state','') == 'end':
            self._stream = None
        await self._client.send(seq=self.seq, **params)

    async def recv(self):
        return await self.__anext__()

    async def kill(self):
        await self.send_q.send(outcome.Error(CancelledError()))
        await self.aclose()

    async def aclose(self, timeout=0.2):
        await self.send_q.aclose()
        if self._stream == 2:
            await self._client.send(seq=self.seq, state='end')
            if timeout is not None:
                with trio.move_on_after(timeout):
                    try:
                        await self.recv()
                    except StopAsyncIteration:
                        return
            req = await self._client.request(action="stop",task=self.seq, _async=True)
            return await req.get()


class _SingleReply:
    """
    This class represents a single-message reply.
    It will delegate itself to a StreamedRequest if a multi message reply
    arrives.
    """
    send_stop = True

    def __init__(self, conn, seq):
        self._conn = conn
        self.seq = seq
        self.q = ValueEvent()

    async def set(self, msg):
        """Called by the read loop to process a command's result"""
        if msg.get('state') == 'start':
            res = StreamedRequest(self._conn, self.seq, stream=None)
            await res.set(msg)
            self.q.set(res)
            return res
        elif 'error' in msg:
            self.q.set_error(ServerError(msg.error))
        else:
            self.q.set(msg)
        return False

    def get(self):
        """Wait for and return the result.
        
        This is a coroutine.
        """
        return self.q.get()
    
    async def kill(self):
        pass


class Client:
    _server_init = None  # Server greeting

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._seq = 0
        self._handlers = {}
        self._send_lock = trio.Lock()

    async def _handle_msg(self, msg):
        try:
            seq = msg.seq
        except AttributeError:
            if 'error' in msg:
                raise RuntimeError("Server error",msg.error)
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

    async def send(self, **params):
        async with self._send_lock:
            await self._socket.send_all(_packer(params))

    async def _reader(self, *, task_status=trio.TASK_STATUS_IGNORED):
        """Main loop for reading
        """
        unpacker = msgpack.Unpacker(object_pairs_hook=attrdict, raw=False, use_list=False)

        with trio.CancelScope(shield=True) as s:
            task_status.started(s)
            try:
                while True:
                    for msg in unpacker:
                        #logger.debug("Recv %s", msg)
                        await self._handle_msg(msg)

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
                    for m in hdl.values():
                        await m.kill()

    async def request(self, action, iter=None, seq=None, _async=False, **params):
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
        params['seq'] = seq
        res = _SingleReply(self, seq)
        self._handlers[seq] = res

        #logger.debug("Send %s", params)
        await self.send(**params)
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
                    raise MoreData(action)
            if rr is None:
                raise NoData(action)
        return res


    @asynccontextmanager
    async def stream(self, action, stream=False, **params):
        """Send and receive a multi-message request.

        Args:
          ``action``: what to do
          ``params``: whatever other data the action needs
          ``stream``: whether to enable multi-line requests
                      via ``await stream.send(**params)``

        This is a context manager. Use it like this::

            async with client.strem("update", path="private storage".split(), stream=True) as req:
                with MsgReader("/tmp/msgs.pack") as f:
                    for msg in f:
                        await req.send(msg)
            # … or …
            async with client.strem("get_tree", path="private storage".split()) as req:
                for msg in req:
                    await process_entry(msg)
            # … or maybe … (auth does this)
            async with client.strem("interactive_thing", path=(None,"foo")) as req:
                msg = await req.recv()
                while msg.get(s,"")=="more":
                    foo.send(s="more",value="some data")
                    msg = await req.recv()
                foo.send(s="that's all then")

        Any server-side exception will be raised on recv.

        The server-side command will be killed if you leave the loop
        without having read a "state=end" message.
        """
        self._seq += 1
        seq = self._seq

        #logger.debug("Send %s", params)
        res = StreamedRequest(self, seq, stream=stream)
        await res.send(action=action, **params)
        try:
            yield res
        except BaseException as exc:
            if stream:
                await res.send(error=str(exc))
            raise
        finally:
            await res.aclose()


    async def _run_auth(self, auth=None):
        hello = self._server_init
        sa = hello.get('auth', ())
        if not sa or not sa[0]:
            # no auth required
            if auth:
                logger.info("Tried to use auth=%s, but not required.", auth._auth_method)
            return
        if not auth:
            raise ClientAuthRequiredError("You need to log in using:", sa[0])
        if auth._auth_method != sa[0]:
            raise ClientAuthMethodError("You cannot use %s" % (auth._auth_method), sa)
        await auth.auth(self)

    @asynccontextmanager
    async def _connected(self, tg, init_timeout=5, auth=None):
        """
        This async context manager handles the actual TCP connection to
        the DistKV server.
        """
        hello = AsyncValueEvent()
        self._handlers[0] = hello
        
        #logger.debug("Conn %s %s",self.host,self.port)
        async with await trio.open_tcp_stream(self.host, self.port) as sock:
            #logger.debug("ConnDone %s %s",self.host,self.port)
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


