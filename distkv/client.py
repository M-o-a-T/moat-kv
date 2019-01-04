# dist-kv client

import anyio
from anyio.exceptions import ClosedResourceError
import outcome
import msgpack
import socket
from async_generator import asynccontextmanager
from aioserf.util import ValueEvent
from .util import attrdict

import logging
logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

__all__ = ['NodData', 'ManyData', 'open_client', 'StreamReply']

class NoData(ValueError):
    """No reply arrived"""

class ManyData(ValueError):
    """More than one reply arrived"""

class ServerError(RuntimeError):
    """The server sent us an error"""

class ServerClosedError(ServerError):
    pass

class ServerConnectionError(ServerError):
    pass

@asynccontextmanager
async def open_client(host, port, init_timeout=5):
    client = Client(host, port)
    async with anyio.create_task_group() as tg:
        async with  client._connected(tg, init_timeout=init_timeout) as client:
            yield client

class _StreamRequest:
    """
    This class represents the core of a multi-message reply.
    """
    result = None

    def __init__(self, client, seq):
        self.client = client
        self.seq = seq

    async def __call__(self, **params):
        params['seq'] = self.seq
        #logger.debug("Send %s", params)
        await self._socket.send_all(_packer(params))

class StreamReply:
    """
    This class represents a multi-message reply.
    """
    send_stop = True
    end_msg = None

    def __init__(self, conn, seq):
        self._conn = conn
        self.seq = seq
        self.q = anyio.create_queue(10000)

    async def set(self, res):
        if 'error' in res:
            await self.q.put(outcome.Error(ServerError(res.error)))
            return
        state = res.get('state', None)
        if state == 'end':
            await self.q.put(None)
            self.q = None
            self.end_msg = res
            return
        elif state is not None:
            logger.warning("Unknown state: %s", repr(state))
        del res['seq']
        await self.q.put(outcome.Value(res))
        return self

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
        return res.unwrap()

    async def cancel(self):
        if self.q is not None:
            await self.q.put(None)



class _SingleReply:
    """
    This class represents a single-message reply.
    It will delegate itself to a StreamReply if a multi message reply
    arrives.
    """
    send_stop = True

    def __init__(self, conn, seq):
        self._conn = conn
        self.seq = seq
        self.q = ValueEvent()

    async def set(self, res):
        if res.get('state') == 'start':
            res = StreamReply(self._conn, self.seq)
            await self.q.set(res)
            return res
        else:
            if 'error' in res:
                await self.q.set_error(ServerError(res.error))
            else:
                await self.q.set(res)

    def get(self):
        return self.q.get()
    
    async def cancel(self):
        pass


class Client:
    _server_init = None  # Server greeting

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._seq = 0
        self._handlers = {}

    async def _handle_msg(self, msg):
        try:
            seq = msg.seq
        except KeyError:
            raise RuntimeError("Reader got out of sync: " + str(msg))
        try:
            hdl = self._handlers.pop(seq)
        except KeyError:
            logger.warn("Spurious message %s: %s", seq, msg)
            return

        res = await hdl.set(msg)
        if res:
            self._handlers[seq] = res


    async def _reader(self):
        """Main loop for reading
        """
        unpacker = msgpack.Unpacker(object_pairs_hook=attrdict, raw=False, use_list=False)

        async with anyio.open_cancel_scope(shield=True) as s:
            try:
                while True:
                    for msg in unpacker:
                        #logger.debug("Recv %s", msg)
                        await self._handle_msg(msg)

                    if self._socket is None:
                        break
                    try:
                        buf = await self._socket.receive_some(4096)
                    except ClosedResourceError:
                        return  # closed by us
                    if len(buf) == 0:  # Connection was closed.
                        raise ServerClosedError("Connection closed by peer")
                    unpacker.feed(buf)

            finally:
                hdl, self._handlers = self._handlers, None
                async with anyio.open_cancel_scope(shield=True):
                    for m in hdl.values():
                        await m.cancel()

    async def request(self, action, iter=None, seq=None, **params):
        """Send a request. Wait for a reply.

        Args:
          ``action``: what to do. If ``seq`` is set, this is the stream's
                      state, which should be ``None`` or ``'end'``.
          ``seq``: Sequence number to use. Only when terminating a
                   multi-message request.
          ``params``: whatever other data the action needs
          ``iter``: A flag how to treat multi-line replies.
                    ``True``: always return an iterator
                    ``False``: Never return an iterator, raise an error
                               if no or more than on reply arrives
                    Default: ``None``: return a StreamReply if multi-line
                                       otherwise return directly
        """
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
        await self._socket.send_all(_packer(params))
        res = await res.get()
        if iter is True and not isinstance(res, StreamReply):
            async def send_one(res):
                yield res
            res = send_one(res)
        elif iter is False and isinstance(res, StreamReply):
            rr = None
            async for r in res:
                if rr is not None:
                    raise MoreData(action)
            if rr is None:
                raise NoData(action)
        return res


    @asynccontextmanager
    async def stream(self, action, iter=None, **params):
        """Send a multi-message request. Wait for a reply at the end.

        Args:
          ``action``: what to do
          ``params``: whatever other data the action needs
          ``iter``: A flag how to treat multi-line replies.
                    ``True``: always return an iterator
                    ``False``: Never return an iterator, raise an error
                               if no or more than on reply arrives
                    Default: ``None``: return a StreamReply if multi-line
                                       otherwise return directly

        This is a context manager. Use it like this::

            async with client.strem("update", path="private storage".split()) as sender:
                with MsgReader("/tmp/msgs.pack") as f:
                    for msg in f:
                        await sender(msg)
            print(sender.result)
            # any server-side exception will be raised

        """
        self._seq += 1
        seq = self._seq

        params['action'] = action
        params['seq'] = seq
        params['state'] = 'start'

        #logger.debug("Send %s", params)
        await self._socket.send_all(_packer(params))

        res = _StreamRequest(self, seq)
        try:
            yield res
        except BaseException as exc:
            await self.request("error", seq=seq, error=str(exc))
            raise
        else:
            res.result = await self.request("end", seq=seq, iter=iter)


    @asynccontextmanager
    async def _connected(self, tg, init_timeout=5):
        """
        This async context manager handles the actual TCP connection to
        the DistKV server.
        """
        hello = ValueEvent()
        self._handlers[0] = hello
        
        #logger.debug("Conn %s %s",self.host,self.port)
        async with await anyio.connect_tcp(self.host, self.port) as sock:
            #logger.debug("ConnDone %s %s",self.host,self.port)
            try:
                self.tg = tg
                self._socket = sock
                await self.tg.spawn(self._reader)
                async with anyio.fail_after(init_timeout):
                    self._server_init = await hello.get()
                yield self
            except socket.error as e:
                raise ServerConnectionError(self.host, self.port) from e
            finally:
                if self._socket is not None:
                    await self._socket.close()
                    self._socket = None
                self.tg.cancel_scope.cancel()
                self.tg = None

