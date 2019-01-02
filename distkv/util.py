from anyio import aopen, open_cancel_scope

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
        for k,v in kv.items():
            if k not in keys:
                keys[k] = []
            keys[k].append(v)
    for k,v in keys.items():
        if len(v) == 1:
            res[k] = v[0]
        elif not isinstance(v[0],Mapping):
            for vv in v[1:]:
                assert not isinstance(vv,Mapping)
            res[k] = v[0]
        else:
            res[k] = combine_dict(*v)
    return res

class attrdict(dict):
    """A dictionary which can be accessed via attributes, for convenience"""
    def __init__(self,*a,**k):
        super(attrdict,self).__init__(*a,**k)
        self._done = set()

    def __getattr__(self,a):
        if a.startswith('_'):
            return super(attrdict,self).__getattr__(a)
        try:
            return self[a]
        except KeyError:
            raise AttributeError(a) from None
    def __setattr__(self,a,b):
        if a.startswith("_"):
            super(attrdict,self).__setattr__(a,b)
        else:
            self[a]=b
    def __delattr__(self,a):
        del self[a]

import yaml
from yaml.representer import SafeRepresenter
SafeRepresenter.add_representer(attrdict, SafeRepresenter.represent_dict)


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

    where the initial number is the passed-in depth.

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
        if res.path[:self.depth] != self.prefix:
            raise RuntimeError("Wrong prefix: has %s, want %s" % (repr(res.path), repr(self.prefix)))

        p = res['path'][self.depth:]
        cdepth = min(len(p), len(self.path))
        for i in range(cdepth):
            if p[i] != self.path[i]:
                cdepth = i
                break
        self.path = p
        p = p[cdepth:]
        res['path'] = p
        res['depth'] = cdepth


class PathLongener:
    """This reverts the operation of a PathShortener.
    """
    def __init__(self, prefix):
        self.depth = len(prefix)
        self.path = prefix

    def __call__(self, res):
        p = res.pop('path', ())
        d = res.pop('depth', None)
        if d is not None:
            p = self.path[:self.depth+d] + p
        self.path = p
        res['path'] = p


class MsgReader:
    """Read a stream of messages from a file.
    
    Usage::

        async with MsgReader("/tmp/msgs.pack") as f:
            async for msg in f:
                process(msg)
    """

    def __init__(self, filename=None, stream=None):
        if (filename is None) == (stream is None):
            raise RuntimeError("You need to specify either filename or stream")
        from .codec import stream_unpacker
        self.fn = filename
        self.fd = stream
        self.unpack = stream_unpacker()

    async def __aenter__(self):
        if self.fd is None:
            self.fd = aopen(self.fn,"rb")
        return self

    async def __aexit__(self, *tb):
        if self.fn is not None:
            await self.fd.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                msg = next(self.unpacker)
            except StopIteration:
                pass
            else:
                return msg
            
            d = await self.fd.read(4096)
            if d == b"":
                raise StopIteration
            unpacker.feed(d)

class MsgWriter:
    """Write a stream of messages from a file.
    
    Usage::

        async with MsgWriter("/tmp/msgs.pack") as f:
            for msg in some_source_of_messages():  # or "async for"
                f(msg)
    """

    def __init__(self, fn, buflen=65536):
        from .codec import stream_unpacker
        self.fn = fn
        self.buf = []
        self.buflen = buflen
        self.curlen = 0

    async def __aenter__(self):
        self.fd = await aopen(fn,"rb")
        return self

    async def __aexit__(self, *tb):
        with open_cancel_scope(shield=True):
            if self.buf:
                await self.fd.write(b''.join(self.buf))
            await self.fd.close()

    async def __call__(self, msg):
        msg = packer(msg)
        self.buf.append(msg)
        self.curlen += len(msg)
        if self.curlen >= self.buflen:
            buf = b''.join(self.buf)
            pos = self.buflen * int(self.curlen/self.buflen)
            await self.fd.write(buf[:pos])
            buf = buf[pos:]
            self.buf = [buf]
            self.curlen = len(buf)

