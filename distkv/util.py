"""
This module contains various helper functions and classes.
"""
import trio
import anyio
import sys
import os
import re
import asyncclick as click

import attr
import outcome
import collections.abc
import simpleeval
import ast as _ast

from getpass import getpass
from collections import deque
from collections.abc import Mapping
from types import ModuleType
from typing import Union, Dict, Optional
from ssl import SSLContext
from functools import partial, total_ordering
from math import log10
from sniffio import current_async_library

import ruamel.yaml as yaml

SafeRepresenter = yaml.representer.SafeRepresenter
SafeConstructor = yaml.constructor.SafeConstructor
Emitter = yaml.emitter.Emitter

NoneType = type(None)

from .exceptions import CancelledError

import logging

logger = logging.getLogger(__name__)


try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager


def singleton(cls):
    return cls()


def yload(stream, multi=False):
    y = yaml.YAML(typ="safe")
    if multi:
        return y.load_all(stream)
    else:
        return y.load(stream)


def yprint(data, stream=sys.stdout, compact=False):
    """
    Standard code to write a YAML record.

    :param data: The data to write.
    :param stream: the file to write to, defaults to stdout.
    :param compact: Write single lines if possible. default False.
    """
    if isinstance(data, (int, float)):
        print(data, file=stream)
    elif isinstance(data, (str, bytes)):
        print(repr(data), file=stream)
    #   elif isinstance(data, bytes):
    #       os.write(sys.stdout.fileno(), data)
    else:
        y = yaml.YAML(typ="safe")
        y.default_flow_style = compact
        y.dump(data, stream=stream)


def yformat(data, compact=None):
    """
    Return ``data`` as a multi-line YAML string.

    :param data: The data to write.
    :param stream: the file to write to, defaults to stdout.
    :param compact: Write single lines if possible. default False.
    """
    from io import StringIO

    s = StringIO()
    yprint(data, compact=compact, stream=s)
    return s.getvalue()


_expect_node = Emitter.expect_node


def expect_node(self, *a, **kw):
    _expect_node(self, *a, **kw)
    self.root_context = False


Emitter.expect_node = _expect_node


class TimeOnlyFormatter(logging.Formatter):
    default_time_format = "%H:%M:%S"
    default_msec_format = "%s.%03d"


class NotGiven:
    """Placeholder value for 'no data' or 'deleted'."""

    def __new__(cls):
        return cls

    def __getstate__(self):
        raise ValueError("You may not serialize this object")

    def __repr__(self):
        return "‹NotGiven›"

    def __str__(self):
        return "NotGiven"


def combine_dict(*d, cls=dict) -> dict:
    """
    Returns a dict with all keys+values of all dict arguments.
    The first found value wins.

    This recurses if values are dicts.

    Args:
      cls (type): a class to instantiate the result with. Default: dict.
        Often used: :class:`attrdict`.
    """
    res = cls()
    keys = {}
    if len(d) <= 1:
        return d
    for kv in d:
        for k, v in kv.items():
            if k not in keys:
                keys[k] = []
            keys[k].append(v)
    for k, v in keys.items():
        if v[0] is NotGiven:
            res.pop(k, None)
        elif len(v) == 1:
            res[k] = v[0]
        elif not isinstance(v[0], Mapping):
            for vv in v[1:]:
                assert vv is NotGiven or not isinstance(vv, Mapping)
            res[k] = v[0]
        else:
            res[k] = combine_dict(*v, cls=cls)
    return res


def drop_dict(data: dict, drop: tuple) -> dict:
    data = data.copy()
    for d in drop:
        vv = data
        if isinstance(d, tuple):
            for dd in d[:-1]:
                vv = vv[dd] = vv[dd].copy()
            d = d[-1]
        del vv[d]
    return data


class attrdict(dict):
    """A dictionary which can be accessed via attributes, for convenience.

    This also supports updating path accessors.
    """

    def __getattr__(self, a):
        if a.startswith("_"):
            return object.__getattribute__(self, a)
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
        try:
            del self[a]
        except KeyError:
            raise AttributeError(a) from None

    def _get(self, path, skip_empty=True, default=NotGiven):
        """
        Get a node's value and access the dict items beneath it.
        """
        if isinstance(path, str):
            raise ValueError(f"Must be a Path/list, not {path!r}")
        val = self
        for p in path:
            if val is None:
                return None
            if skip_empty and not p:
                continue
            val = val.get(p, NotGiven)
            if val is NotGiven:
                if default is NotGiven:
                    raise KeyError(path)
                return default
        return val

    def _update(self, path, value=None, skip_empty=True):
        """
        Set some sub-item's value, possibly merging dicts.
        Items set to 'NotGiven' are deleted.

        Returns the new value. Modified (sub)dicts will be copied.
        """
        if isinstance(path, str):
            raise ValueError(f"Must be a Path/list, not {path!r}")
        if skip_empty:
            path = [p for p in path if p]
        val = type(self)(**self)
        v = val
        if not path:
            if isinstance(value, Mapping):
                return combine_dict(value, val, cls=type(self))
            else:
                return value

        for p in path[:-1]:
            try:
                w = v[p]
            except KeyError:
                w = type(v)()
            else:
                # copy
                w = type(w)(w)
            v[p] = w
            v = w
        px = path[-1]
        if value is NotGiven:
            v.pop(px, None)
        elif not isinstance(value, Mapping):
            v[px] = value
        elif px in v:
            v[px] = combine_dict(value, v[px], cls=type(self))
        else:
            v[px] = value

        return val

    def _delete(self, path, skip_empty=True):
        """
        Remove some sub-item's value, possibly removing now-empty intermediate
        dicts.

        Returns the new value. Modified (sub)dicts will be copied.
        """
        if isinstance(path, str):
            raise ValueError(f"Must be a Path/list, not {path!r}")
        if skip_empty:
            path = [p for p in path if p]
        val = type(self)(**self)
        v = val
        vc = []
        for p in path[:-1]:
            vc.append(v)
            try:
                w = v[p]
            except KeyError:
                return self
            w = type(w)(**w)
            v[p] = w
            v = w
        vc.append(v)
        while path:
            v = vc.pop()
            del v[path.pop()]
            if v:
                break
        return val


SafeRepresenter.add_representer(attrdict, SafeRepresenter.represent_dict)


def str_presenter(dumper, data):
    if "\n" in data:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


SafeRepresenter.add_representer(str, str_presenter)


def count(it):
    n = 0
    for _ in it:
        n += 1
    return n


async def acount(it):
    n = 0
    async for _ in it:  # noqa: F841
        n += 1
    return n


class _MsgRW:
    """
    Common base class for :class:`MsgReader` and :class:`MsgWriter`.
    """

    _mode = None

    def __init__(self, path=None, stream=None):
        if (path is None) == (stream is None):
            raise RuntimeError("You need to specify either path or stream")
        self.path = path
        self.stream = stream

    async def __aenter__(self):
        if self.path is not None:
            self.stream = await anyio.aopen(self.path, self._mode)
        return self

    async def __aexit__(self, *tb):
        if self.path is not None:
            async with anyio.open_cancel_scope(shield=True):
                try:
                    await self.stream.aclose()
                except AttributeError:
                    await self.stream.close()


class MsgReader(_MsgRW):
    """Read a stream of messages (encoded with MsgPack) from a file.

    Usage::

        async with MsgReader(path="/tmp/msgs.pack") as f:
            async for msg in f:
                process(msg)

    Arguments:
      buflen (int): The read buffer size. Defaults to 4k.
      path (str): the file to write to.
      stream: the stream to write to.

    Exactly one of ``path`` and ``stream`` must be used.
    """

    _mode = "rb"

    def __init__(self, *a, buflen=4096, **kw):
        super().__init__(*a, **kw)
        self.buflen = buflen

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

            d = await self.stream.read(self.buflen)
            if d == b"":
                raise StopAsyncIteration
            self.unpack.feed(d)


packer = None


class MsgWriter(_MsgRW):
    """Write a stream of messages to a file (encoded with MsgPack).

    Usage::

        async with MsgWriter("/tmp/msgs.pack") as f:
            for msg in some_source_of_messages():  # or "async for"
                await f(msg)

    Arguments:
      buflen (int): The buffer size. Defaults to 64k.
      path (str): the file to write to.
      stream: the stream to write to.

    Exactly one of ``path`` and ``stream`` must be used.

    The stream is buffered. Call :meth:`distkv.util.MsgWriter.flush` to flush the buffer.
    """

    _mode = "wb"

    def __init__(self, *a, buflen=65536, **kw):
        super().__init__(*a, **kw)

        self.buf = []
        self.buflen = buflen
        self.curlen = 0
        self.excess = 0

        global packer  # pylint: disable=global-statement
        if packer is None:
            from .codec import packer  # pylint: disable=redefined-outer-name

    async def __aexit__(self, *tb):
        async with anyio.fail_after(2, shield=True):
            if self.buf:
                await self.stream.write(b"".join(self.buf))
            await super().__aexit__(*tb)

    async def __call__(self, msg):
        """Write a message (bytes) to the buffer.

        Flushing writes a multiple of ``buflen`` bytes."""
        msg = packer(msg)  # pylint: disable=not-callable
        self.buf.append(msg)
        self.curlen += len(msg)
        if self.curlen + self.excess >= self.buflen:
            buf = b"".join(self.buf)
            pos = self.buflen * int((self.curlen + self.excess) / self.buflen)
            assert pos > 0
            wb, buf = buf[:pos], buf[pos:]
            self.curlen = len(buf)
            self.buf = [buf]
            self.excess = 0
            await self.stream.write(wb)

    async def flush(self):
        """Flush the buffer."""
        if self.buf:
            buf = b"".join(self.buf)
            self.buf = []
            self.excess = (self.excess + len(buf)) % self.buflen
            await self.stream.write(buf)


class _Server:
    _servers = None
    recv_q = None

    def __init__(self, tg, port=0, ssl=None, **kw):
        self.tg = tg
        self.port = port
        self.ports = None
        self._kw = kw
        self.ssl = ssl

    async def _accept(self, server, q):
        self.ports.append(server.socket.getsockname())
        try:
            while True:
                conn = await server.accept()
                if self.ssl:
                    conn = trio.SSLStream(conn, self.ssl, server_side=True)
                await q.send(conn)
        finally:
            async with anyio.fail_after(2, shield=True):
                await q.aclose()
                await server.aclose()

    async def __aenter__(self):
        if current_async_library() != "trio":
            raise RuntimeError("This only works with Trio right now.Sorry.")
        send_q, self.recv_q = trio.open_memory_channel(1)
        try:
            servers = await trio.open_tcp_listeners(self.port, **self._kw)
        except EnvironmentError as exc:
            err = OSError(f"Port {self.port} in use")
            err.errno = exc.errno
            raise err from exc

        self.ports = []
        async with send_q:
            for s in servers:
                await self.tg.spawn(self._accept, s, send_q.clone())
        return self

    async def __aexit__(self, *tb):
        await self.tg.cancel_scope.cancel()
        async with anyio.fail_after(2, shield=True):
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
    async with anyio.create_task_group() as tg:
        server = _Server(tg, **args)
        async with server:
            yield server


def gen_ssl(
    ctx: Union[bool, SSLContext, Dict[str, str]] = False, server: bool = True
) -> Optional[SSLContext]:
    """
    Generate a SSL config from the given context.

    Args:
      ctx: either a Bool (ssl yes/no) or a dict with "key" and "cert" entries.
      server: a flag whether to behave as a server.
    """
    if not ctx:
        return None
    if ctx is True:
        ctx = dict()
    if not isinstance(ctx, dict):
        return ctx

    # pylint: disable=no-member
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


def _call_proc(code, variables, *a, **kw):
    v = variables[len(a) :]
    if v:
        a = list(a)
        for k in v:
            a.append(kw.pop(k, None))
    eval(code, kw)  # pylint: disable=eval-used
    code = kw["_proc"]
    return code(*a)


def make_proc(code, variables, path, *, use_async=False):  # pylint: disable=redefined-builtin
    """Compile this code block to a procedure.

    Args:
        code: the code block to execute. Text, will be indented.
        vars: variable names to pass into the code
        path: the location where the code is stored
        use_async: False if sync code, True if async, None if in thread
    Returns:
        the procedure to call. All keyval arguments will be in the local
        dict.
    """
    hdr = f"""\
def _proc({ ",".join(variables) }):
    """

    if use_async:
        hdr = "async " + hdr
    code = hdr + code.replace("\n", "\n    ")
    code = compile(code, str(path), "exec")

    return partial(_call_proc, code, variables)


class Module(ModuleType):
    def __repr__(self):
        return "<Module %s>" % (self.__class__.__name__,)


def make_module(code, path):
    """Compile this code block to something module-ish.

    Args:
        code: the code block to execute
        path: the location where the code is / shall be stored
    Returns:
        the procedure to call. All keyval arguments will be in the local
        dict.
    """
    name = ".".join(str(x) for x in path)
    code = compile(code, name, "exec")
    m = sys.modules.get(name, None)
    if m is None:
        m = ModuleType(name)
    eval(code, m.__dict__)  # pylint: disable=eval-used
    sys.modules[name] = m
    return m


class Cache:
    """
    A quick-and-dirty cache that keeps the last N entries of anything
    in memory so that ref and WeakValueDictionary don't lose them.

    Entries get refreshed when they're in the last third of the cache; as
    they're not removed, the actual cache size might only be 2/3rd of SIZE.
    """

    def __init__(self, size):
        self._size = size
        self._head = 0
        self._tail = 0
        self._attr = "_cache__pos"
        self._q = deque()

    def keep(self, entry):
        if getattr(entry, self._attr, -1) > self._tail + self._size / 3:
            return
        self._head += 1
        setattr(entry, self._attr, self._head)
        self._q.append(entry)
        self._flush()

    def _flush(self):
        while self._head - self._tail > self._size:
            self._q.popleft()
            self._tail += 1

    def resize(self, size):
        """Change the size of this cache.
        """
        self._size = size
        self._flush()

    def clear(self):
        while self._head > self._tail:
            self._q.popleft()
            self._tail += 1


@singleton
class NoLock:
    """A dummy singleton that can replace a lock.

    Usage::

        with NoLock if _locked else self._lock:
            pass
    """

    async def __aenter__(self):
        return self

    async def __aexit__(self, *tb):
        return


async def data_get(
    obj,
    path,
    *,
    recursive=True,
    as_dict="_",
    maxdepth=-1,
    mindepth=0,
    empty=False,
    raw=False,
    internal=False,
    path_mangle=None,
    item_mangle=None,
):
    """Generic code to dump a subtree.

    `path_mangle` accepts a path and the as_dict parameter. It should
    return the new path. This is used for e.g. prefixing the path with a
    device name. Returning ``None`` causes the entry to be skipped.
    """
    if path_mangle is None:
        path_mangle = lambda x: x
    if item_mangle is None:

        async def item_mangle(x):  # pylint: disable=function-redefined
            return x

    if recursive:
        kw = {}
        if maxdepth is not None:
            kw["max_depth"] = maxdepth
        if mindepth is not None:
            kw["min_depth"] = mindepth
        if empty:
            kw["add_empty"] = True
        if obj.meta:
            kw.setdefault("nchain", obj.meta)
        y = {}
        if internal:
            res = await obj.client._request(action="get_tree_internal", path=path, iter=True, **kw)
        else:
            res = obj.client.get_tree(path, nchain=obj.meta, **kw)
        async for r in res:
            r = await item_mangle(r)
            if r is None:
                continue
            r.pop("seq", None)
            path = r.pop("path")
            path = path_mangle(path)
            if path is None:
                continue

            if as_dict is not None:
                yy = y
                for p in path:
                    yy = yy.setdefault(p, {})
                try:
                    yy[as_dict] = r if obj.meta else r.value
                except AttributeError:
                    if empty:
                        yy[as_dict] = None
            else:
                if raw:
                    y = path
                else:
                    y = {}
                    try:
                        y[path] = r if obj.meta else r.value
                    except AttributeError:
                        if empty:
                            y[path] = None
                        else:
                            continue
                yprint([y], stream=obj.stdout)

        if as_dict is not None:
            if maxdepth:

                def simplex(d):
                    for k, v in d.items():
                        if isinstance(v, dict):
                            d[k] = simplex(d[k])
                    if as_dict in d and d[as_dict] is None:
                        if len(d) == 1:
                            return None
                        else:
                            del d[as_dict]
                    return d

                y = simplex(y)
            yprint(y, stream=obj.stdout)
        return

    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    if as_dict is not None:
        raise click.UsageError("'as-dict' only works with 'recursive'")
    res = await obj.client.get(path, nchain=obj.meta)
    if not obj.meta:
        try:
            res = res.value
        except AttributeError:
            if obj.debug:
                print("No data at", path, file=sys.stderr)
            sys.exit(1)

    if not raw:
        yprint(res, stream=obj.stdout)
    elif isinstance(res, bytes):
        os.write(obj.stdout.fileno(), res)
    else:
        obj.stdout.write(str(res))


@asynccontextmanager
async def as_service(obj=None):
    """
    This async context manager provides readiness and keepalive messages to
    systemd.

    Arguments:
        obj: command context. Needs a ``debug`` attribute.

    The CM yields a (duck-typed) event whose async ``set`` method will
    trigger a ``READY=1`` mesage to systemd.
    """
    from systemd.daemon import notify  # pylint: disable=no-name-in-module

    async def run_keepalive(usec):
        usec /= 1_500_000  # 2/3rd of usec ⇒ sec
        pid = os.getpid()
        while os.getpid() == pid:
            notify("WATCHDOG=1")
            await anyio.sleep(usec)

    def need_keepalive():
        pid = os.getpid()
        epid = int(os.environ.get("WATCHDOG_PID", pid))
        if pid == epid:
            return int(os.environ.get("WATCHDOG_USEC", 0))

    class RunMsg:
        def __init__(self, obj):
            self.obj = obj

        async def set(self):
            notify("READY=1")
            if self.obj is not None and self.obj.debug:
                print("Running.")

    async with anyio.create_task_group() as tg:
        usec = need_keepalive()
        if usec:
            await tg.spawn(run_keepalive, usec)
        try:
            yield RunMsg(obj)
        finally:
            async with anyio.fail_after(2, shield=True):
                await tg.cancel_scope.cancel()


@attr.s
class ValueEvent:
    """A waitable value useful for inter-task synchronization,
    inspired by :class:`threading.Event`.

    An event object manages an internal value, which is initially
    unset, and a task can wait for it to become True.

    Args:
      ``scope``:  A cancelation scope that will be cancelled if/when
                  this ValueEvent is. Used for clean cancel propagation.

    Note that the value can only be read once.
    """

    event = attr.ib(factory=anyio.create_event, init=False)
    value = attr.ib(default=None, init=False)
    scope = attr.ib(default=None, init=True)

    async def set(self, value):
        """Set the result to return this value, and wake any waiting task.
        """
        self.value = outcome.Value(value)
        await self.event.set()

    async def set_error(self, exc):
        """Set the result to raise this exceptio, and wake any waiting task.
        """
        self.value = outcome.Error(exc)
        await self.event.set()

    def is_set(self):
        """Check whether the event has occurred.
        """
        return self.value is not None

    async def cancel(self):
        """Send a cancelation to the recipient.

        TODO: Trio can't do that cleanly.
        """
        if self.scope is not None:
            await self.scope.cancel()
        await self.set_error(CancelledError())

    async def get(self):
        """Block until the value is set.

        If it's already set, then this method returns immediately.

        The value can only be read once.
        """
        await self.event.wait()
        return self.value.unwrap()


async def spawn(taskgroup, proc, *args, **kw):
    """
    Run a task within this object's task group.

    Returns:
        a cancel scope you can use to stop the task.
    """

    scope = None

    async def _run(proc, args, kw, evt):
        """
        Helper for starting a task.

        This accepts a :class:`ValueEvent`, to pass the task's cancel scope
        back to the caller.
        """
        nonlocal scope
        async with anyio.open_cancel_scope() as sc:
            scope = sc
            await evt.set()
            await proc(*args, **kw)

    evt = anyio.create_event()
    await taskgroup.spawn(_run, proc, args, kw, evt)
    await evt.wait()
    return scope


_PartRE = re.compile("[^:._]+|_|:|\\.")


@total_ordering
class Path(collections.abc.Sequence):
    """
    This object represents the path to a DistKV node.

    It is an immutable list with special representation.
    """

    def __init__(self, *a):
        self._data = a

    @classmethod
    def build(cls, data):
        """Optimized shortcut to generate a path from an existing tuple"""
        if isinstance(data, Path):
            return data
        if not isinstance(data, tuple):
            return cls(*data)
        p = object.__new__(cls)
        p._data = data
        return p

    def __str__(self):
        def _escol(x, spaces=True):  # XXX make the default adjustable?
            x = x.replace(":", "::").replace(".", ":.")
            if spaces:
                x = x.replace(" ", ":_")
            return x

        res = []
        if not self._data:
            return ":"
        for x in self._data:
            if isinstance(x, str) and len(x):
                if res:
                    res.append(".")
                res.append(_escol(x))
            elif isinstance(x, (Path, tuple)) and len(x):
                x = ",".join(repr(y) for y in x)
                res.append(":" + _escol(x))
            elif x is True:
                res.append(":t")
            elif x is False:
                res.append(":f")
            elif x is None:
                res.append(":n")
            elif x == "":
                res.append(":e")
            else:
                if isinstance(x, (Path, tuple)):  # no spaces
                    assert not len(x)
                    x = "()"
                else:
                    x = repr(x)
                res.append(":" + _escol(x))
        return "".join(res)

    def __getitem__(self, x):
        if isinstance(x, slice) and x.start in (0, None) and x.step in (1, None):
            return type(self)(*self._data[x])
        else:
            return self._data[x]

    def __len__(self):
        return len(self._data)

    def __bool__(self):
        return True

    def __eq__(self, other):
        if isinstance(other, Path):
            other = other._data
        return self._data == other

    def __lt__(self, other):
        if isinstance(other, Path):
            other = other._data
        return self._data < other

    def __hash__(self):
        return hash(self._data)

    def __iter__(self):
        return self._data.__iter__()

    def __contains__(self, x):
        return x in self._data

    def __or__(self, other):
        return Path(*self._data, other)

    def __add__(self, other):
        if isinstance(other, Path):
            other = other._data
        if not len(other):
            return self
        return Path(*self._data, *other)

    # TODO add alternate output with hex integers

    def __repr__(self):
        return "P(%r)" % (str(self),)

    @classmethod
    def from_str(cls, path):
        """
        Constructor to build a Path from its string representation.
        """
        res = []
        part: Union[NoneType, bool, str] = False
        # non-empty string: accept colon-eval or dot (inline)
        # True: require dot or colon-eval (after :t)
        # False: accept only colon-eval (start)
        # None: accept neither (after dot)

        esc: bool = False
        # marks that an escape char has been seen

        eval_: Union[bool, int] = False
        # marks whether the current input shall be evaluated;
        # 2=it's a hex number

        pos = 0
        if isinstance(path, (tuple, list)):
            return cls.build(path)
        if path == ":":
            return cls()

        def add(x):
            nonlocal part
            if not isinstance(part, str):
                part = ""
            try:
                part += x
            except TypeError:
                raise SyntaxError(f"Cannot add {x!r} at {pos}")

        def done(new_part):
            nonlocal part
            nonlocal eval_
            if isinstance(part, str):
                if eval_:
                    try:
                        if eval_ == 2:
                            part = int(part, 16)
                        else:
                            part = path_eval(part)
                    except Exception:
                        raise SyntaxError(f"Cannot eval {part!r} at {pos}")
                    eval_ = False
                res.append(part)
            part = new_part

        def new(x, new_part):
            nonlocal part
            if part is None:
                raise SyntaxError(f"Cannot use {part!r} at {pos}")
            done(new_part)
            res.append(x)

        if path == "":
            raise SyntaxError("The empty string is not a path")
        for e in _PartRE.findall(path):
            if esc:
                esc = False
                if e in ":.":
                    add(e)
                elif e == "e":
                    new("", True)
                elif e == "t":
                    new(True, True)
                elif e == "f":
                    new(False, True)
                elif e == "n":
                    new(None, True)
                elif e == "_":
                    add(" ")
                elif e[0] == "x":
                    done(None)
                    part = e[1:]
                    eval_ = 2
                else:
                    if part is None:
                        raise SyntaxError(f"Cannot parse {path!r} at {pos}")
                    done("")
                    add(e)
                    eval_ = True
            else:
                if e == ".":
                    if not part:
                        raise SyntaxError(f"Cannot parse {path!r} at {pos}")
                    done(None)
                    pos += 1
                    continue
                elif e == ":":
                    esc = True
                    pos += 1
                    continue
                elif part is True:
                    raise SyntaxError(f"Cannot parse {path!r} at {pos}")
                else:
                    add(e)
            pos += len(e)
        if esc or part is None:
            raise SyntaxError(f"Cannot parse {path!r} at {pos}")
        done(None)
        return cls(*res)

    @classmethod
    def _make(cls, loader, node):
        value = loader.construct_scalar(node)
        return cls.from_str(value)


P = Path.from_str


class PathNode(yaml.nodes.ScalarNode):
    pass


def _path_repr(dumper, data):
    return dumper.represent_scalar("!P", str(data))
    # return ScalarNode(tag, value, style=style)
    # return yaml.events.ScalarEvent(anchor=None, tag='!P', implicit=(True, True), value=str(data))


SafeRepresenter.add_representer(Path, _path_repr)
SafeConstructor.add_constructor("!P", Path._make)


def _bin_from_ascii(loader, node):
    value = loader.construct_scalar(node)
    return value.encode("ascii")


def _bin_to_ascii(dumper, data):
    try:
        data = data.decode("ascii")
    except UnicodeError:
        return dumper.represent_binary(data)
    else:
        return dumper.represent_scalar("!bin", data)


SafeRepresenter.add_representer(bytes, _bin_to_ascii)
SafeConstructor.add_constructor("!bin", _bin_from_ascii)


# path_eval is a simple "eval" replacement to implement resolving
# expressions in paths. While it can be used for math its primary function
_eval = simpleeval.SimpleEval(functions={})
_eval.nodes[_ast.Tuple] = lambda node: tuple(_eval._eval(x) for x in node.elts)
path_eval = _eval.eval


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

    Note that the input dict is modified in-place.

    """

    def __init__(self, prefix):
        self.prefix = prefix
        self.depth = len(prefix)
        self.path = []

    def __call__(self, res):
        try:
            p = res["path"]
        except KeyError:
            return
        if list(p[: self.depth]) != list(self.prefix):
            raise RuntimeError(f"Wrong prefix: has {p!r}, want {self.prefix!r}")

        p = p[self.depth :]  # noqa: E203
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
    """
    This reverts the operation of a PathShortener. You need to pass the
    same prefix in.

    Calling a PathLongener with a dict without ``depth`` or ``path``
    attributes is a no-op.
    """

    def __init__(self, prefix: Union[Path, tuple] = ()):
        self.depth = len(prefix)
        self.path = Path.build(prefix)

    def __call__(self, res):
        p = res.get("path", None)
        if p is None:
            return
        d = res.pop("depth", None)
        if d is None:
            return
        if not isinstance(p, tuple):
            # may be a list, dammit
            p = tuple(p)
        p = self.path[: self.depth + d] + p
        self.path = p
        res["path"] = p


def res_get(res, attr: Path, **kw):  # pylint: disable=redefined-outer-name
    """
    Get a node's value and access the dict items beneath it.

    The node value must be an attrdict.
    """
    val = res.get("value", None)
    if val is None:
        return None
    return val._get(attr, **kw)


def res_update(res, attr: Path, value=None, **kw):  # pylint: disable=redefined-outer-name
    """
    Set a node's sub-item's value, possibly merging dicts.
    Entries set to 'NotGiven' are deleted.

    The node value must be an attrdict.

    Returns the new value.
    """
    val = res.get("value", attrdict())
    return val._update(attr, value=value, **kw)


def res_delete(res, attr: Path, **kw):  # pylint: disable=redefined-outer-name
    """
    Remove a node's sub-item's value, possibly removing now-empty
    intermediate dicts.

    The node value must be an attrdict.

    Returns the new value.
    """
    val = res.get("value", attrdict())
    return val._delete(attr, **kw)


def logger_for(path: Path):
    """
    Create a logger for this ``path``.
    """
    if not len(path):
        p = "distkv.root"
    elif path[0] is None:
        p = "distkv.meta"
    elif path[0] == ".distkv":
        p = "distkv.sub"
    else:
        p = "distkv.at"
    if len(path) > 1:
        p += "." + ".".join(path[1:])
    return logging.getLogger(p)


def digits(n, digits=6):  # pylint: disable=redefined-outer-name
    """
    Returns ``n`` rounded to ``digits`` significant digits. Default: 6.
    Ensures that the number doesn't carry nonsense precision or
    floating-point artefacts.

    >>> digits(123456789, 4)
    123400000
    >>> digits(math.pi, 4)
    3.142

    ``digits`` may be a fraction, in order to move the cut-off point to
    somewhere other than between 9.999 and 10.00.
    """
    return round(n, int(digits - 1 - log10(abs(n))))
