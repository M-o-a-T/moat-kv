import trio
from getpass import getpass
from collections.abc import Mapping

import logging

try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager


def combine_dict(*d):
    """
    Returns a dict with all keys+values of all dict arguments.
    The first found value wins.

    This recurses if values are dicts.
    """
    res = {}
    keys = {}
    if len(d) <= 1:
        return d
    for kv in d:
        for k, v in kv.items():
            if k not in keys:
                keys[k] = []
            keys[k].append(v)
    for k, v in keys.items():
        if len(v) == 1:
            res[k] = v[0]
        elif not isinstance(v[0], Mapping):
            for vv in v[1:]:
                assert not isinstance(vv, Mapping)
            res[k] = v[0]
        else:
            res[k] = combine_dict(*v)
    return res


class attrdict(dict):
    """A dictionary which can be accessed via attributes, for convenience"""

    def __init__(self, *a, **k):
        super(attrdict, self).__init__(*a, **k)
        self._done = set()

    def __getattr__(self, a):
        if a.startswith("_"):
            return super(attrdict, self).__getattr__(a)
        try:
            return self[a]
        except KeyError:
            raise AttributeError(a) from None

    def __setattr__(self, a, b):
        if a.startswith("_"):
            super(attrdict, self).__setattr__(a, b)
        else:
            self[a] = b

    def __delattr__(self, a):
        del self[a]


from yaml.representer import SafeRepresenter

SafeRepresenter.add_representer(attrdict, SafeRepresenter.represent_dict)


def count(iter):
    n = 0
    for _ in iter:
        n += 1
    return n


async def acount(iter):
    n = 0
    async for _ in iter:  # noqa: F841
        n += 1
    return n


class PathShortener:
    """This class shortens path entries so that the initial components that
    are equal to the last-used path (or the original base) are skipped.

    It is illegal to path-shorten messages whose path does not start with
    the initial prefix.

    Example: The sequence

        a b
        a b c d
        a b c e f
        a b c e g h
        a b c i
        a b j

    is shortened to

        0
        0 c d
        1 e f
        2 g h
        1 i
        0 j

    where the initial number is the passed-in ``depth``, assuming the
    PathShortener is initialized with ``('a','b')``.

    Usage::

        >>> d = _PathShortener(['a','b'])
        >>> d({'path': 'a b c d'.split})
        {'depth':0, 'path':['c','d']}
        >>> d({'path': 'a b c e f'.split})
        {'depth':1, 'path':['e','f']}

    etc.

    Note that the dict is modified in-place.

    """

    def __init__(self, prefix):
        self.prefix = prefix
        self.depth = len(prefix)
        self.path = []

    def __call__(self, res):
        if res.path[: self.depth] != self.prefix:
            raise RuntimeError(
                "Wrong prefix: has %s, want %s" % (repr(res.path), repr(self.prefix))
            )

        p = res["path"][self.depth :]  # noqa: E203
        cdepth = min(len(p), len(self.path))
        for i in range(cdepth):
            if p[i] != self.path[i]:
                cdepth = i
                break
        self.path = p
        p = p[cdepth:]
        res["path"] = p
        res["depth"] = cdepth


class PathLongener:
    """This reverts the operation of a PathShortener. You need to pass the
    same prefix in.
    """

    def __init__(self, prefix):
        self.depth = len(prefix)
        self.path = prefix

    def __call__(self, res):
        try:
            p = res.path
        except AttributeError:
            return
        d = res.pop("depth", None)
        if d is not None:
            p = self.path[: self.depth + d] + p
        self.path = p
        res["path"] = p


class _MsgRW:
    def __init__(self, path=None, stream=None):
        if (path is None) == (stream is None):
            raise RuntimeError("You need to specify either path or stream")
        self.path = trio.Path(path)
        self.stream = stream

    async def __aenter__(self):
        if self.path is not None:
            self.stream = await self.path.open(self._mode)
        return self

    async def __aexit__(self, *tb):
        if self.path is not None:
            with trio.CancelScope(shield=True):
                await self.stream.aclose()


class MsgReader(_MsgRW):
    """Read a stream of messages from a file.

    Usage::

        async with MsgReader("/tmp/msgs.pack") as f:
            async for msg in f:
                process(msg)
    """

    _mode = "rb"

    def __init__(self, stream, **kw):
        super().__init__(**kw)
        from .codec import stream_unpacker

        self.unpack = stream_unpacker()

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                msg = next(self.unpack)
            except StopIteration:
                pass
            else:
                return msg

            d = await self.stream.read(4096)
            if d == b"":
                raise StopAsyncIteration
            self.unpack.feed(d)


packer = None


class MsgWriter(_MsgRW):
    """Write a stream of messages from a file.

    Usage::

        async with MsgWriter("/tmp/msgs.pack") as f:
            for msg in some_source_of_messages():  # or "async for"
                f(msg)
    """

    _mode = "wb"

    def __init__(self, buflen=65536, **kw):
        super().__init__(**kw)

        self.buf = []
        self.buflen = buflen
        self.curlen = 0
        self.excess = 0

        global packer
        if packer is None:
            from .codec import packer

    async def __aexit__(self, *tb):
        with trio.CancelScope(shield=True):
            if self.buf:
                await self.stream.write(b"".join(self.buf))
            await super().__aexit__(*tb)

    async def __call__(self, msg):
        msg = packer(msg)
        self.buf.append(msg)
        self.curlen += len(msg)
        if self.curlen >= self.buflen - self.excess:
            buf = b"".join(self.buf)
            pos = self.buflen * int(self.curlen / self.buflen) - self.excess
            assert pos > 0
            wb, buf = buf[:pos], buf[pos:]
            self.buf = [buf]
            self.curlen = len(buf)
            self.excess = 0
            await self.stream.write(wb)

    async def flush(self):
        if self.buf:
            buf = b"".join(self.buf)
            self.buf = []
            self.excess = (self.excess + len(buf)) % self.buflen
            await self.stream.write(buf)


class TimeOnlyFormatter(logging.Formatter):
    default_time_format = "%H:%M:%S"
    default_msec_format = "%s.%03d"


class Queue:
    def __init__(self, len):
        self._send, self._recv = trio.open_memory_channel(len)

    async def get(self):
        return await self._recv.receive()

    async def put(self, msg):
        await self._send.send(msg)

    async def aclose(self):
        await self._send.aclose()


class _Server:
    _servers = None
    _q = None

    def __init__(self, tg, port=0, ssl=None, **kw):
        self.tg = tg
        self.port = port
        self.ports = None
        self._kw = kw
        self.ssl = ssl

    async def _accept(self, server, q, *, task_status=trio.TASK_STATUS_IGNORED):
        self.ports.append(server.socket.getsockname())
        task_status.started()
        try:
            while True:
                conn = await server.accept()
                if self.ssl:
                    conn = trio.SSLStream(conn, self.ssl, server_side=True)
                await q.send(conn)
        finally:
            with trio.CancelScope(shield=True):
                await q.aclose()
                await server.aclose()

    async def __aenter__(self):
        send_q, self.recv_q = trio.open_memory_channel(1)
        servers = await trio.open_tcp_listeners(self.port, **self._kw)
        self.ports = []
        for s in servers:
            await self.tg.start(self._accept, s, send_q.clone())
        await send_q.aclose()
        return self

    async def __aexit__(self, *tb):
        self.tg.cancel_scope.cancel()
        with trio.CancelScope(shield=True):
            await self.recv_q.aclose()

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.recv_q.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration


@asynccontextmanager
async def create_tcp_server(**args) -> _Server:
    async with trio.open_nursery() as tg:
        server = _Server(tg, **args)
        async with server:
            yield server


def gen_ssl(ctx, server: bool = True):
    if not ctx:
        return None
    if ctx is True:
        ctx = dict()
    if not isinstance(ctx, dict):
        return ctx

    ctx_ = trio.ssl.create_default_context(
        purpose=trio.ssl.Purpose.CLIENT_AUTH if server else trio.ssl.Purpose.SERVER_AUTH
    )
    if "key" in ctx:
        ctx_.load_cert_chain(ctx["cert"], ctx["key"])
    return ctx_


def num2byte(num: int, length=None):
    if length is None:
        length = (num.bit_length() + 7) // 8
    return num.to_bytes(length=length, byteorder="big")


def byte2num(data: bytes):
    return int.from_bytes(data, byteorder="big")


def split_one(p, kw):
    """Split 'p' and add to dict 'kw'."""
    try:
        k, v = p.split("=", 1)
    except ValueError:
        if p[-1] == "?":
            k = p[:-1]
            v = getpass(k + "? ")
        else:
            raise
    else:
        if k[-1] == "?":
            k = k[:-1]
            v = getpass(v + ": ")
        try:
            v = int(v)
        except ValueError:
            pass
    kw[k] = v


def make_proc(code, vars, *path, use_async=False):
    """Compile this code block to a procedure.

    Args:
        code: the code block to execute
        vars: variable names to pass into the code
        path: the location where the code is / shall be stored
    Returns:
        the procedure to call. All keyval arguments will be in the local
        dict.
    """
    vars = ",".join(vars)
    if vars:
        vars += ","
    hdr = """\
def proc(%s **kw):
    locals().update(kw)
    """ % (
        vars,
    )

    if use_async:
        hdr = "async " + hdr
    code = hdr + code.replace("\n", "\n\t")
    code = compile(code, ".".join(str(x) for x in path), "exec")
    d = {}
    eval(code, d)
    code = d["proc"]
    return code
