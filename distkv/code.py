"""
This module locally stores code that's been uploaded to DistKV 
so that it can be called easily.

"Code" consists of either Python modules or single procedures.

"""

import sys
import anyio
from collections import defaultdict, deque
from weakref import WeakValueDictionary
from time import time  # wall clock, intentionally

from .util import PathLongener, make_module, make_proc
from .client import ClientRoot, ClientEntry

import logging
logger = logging.getLogger(__name__)

CFG = {
        'prefix': ("code",),
        }

class EmptyCode(Exception):
    """
    There is no code here.
    """
    pass

class ModuleRoot(ClientRoot):
    """
    Modules are stored at ``(*PREFIX,*name)``.
    The prefix defaults to ``("code","module")``.

    The module code is stored textually. Content that is not UTF-8 is TODO.
    """
    async def run(self, *a):
        self.errors = await self.client.unique_helper(("error",))
        await super().run(*a)

    @classmethod
    def child_type(cls, name):
        return ModuleEntry

class ModuleEntry(ClientEntry):
    @property
    def name(self):
        return '.'.join(self.subpath)

    async def set_value(self, value):
        await super().set_value(value)
        if value is None:
            self._module = None
            del sys.modules[self.name]
            return

        try:
            if not isinstance(self.value, str):
                raise RuntimeError("Not a string, cannot compile")
            m = make_module(self.value, *self.subpath)
        except Exception as exc:
            logger.warn("Could not compile @%r", self.subpath)
            await self.root.errors.record_exc("compile", *self.subpath,
                    exc=exc, reason="compilation", message="compiler error")
        else:
            await self.root.errors.record_working("compile", *self.subpath)


class ProcRoot(ClientRoot):
    """
    Single procedures are stored at ``(*PREFIX,*name)``.
    The prefix defaults to ``("code","proc")``.

    The code is stored as a dict.

    Internally the code is prefixed with "def proc()".

    Arguments:
      code: the actual code to run.
      async: flag whether the code should run asynchronously.
      vars: array of names to be used as named arguments.

    All arguments are mandatory and should be named.
    Extra arguments will be available in the "kw" dict.
    """

    @classmethod
    def child_type(cls, name):
        return ProcEntry

    async def run_starting(self):
        from .errors import get_error_handler
        self._err = await get_error_handler(self.client)
        await super().run_starting()

    async def add(self, *path, code=None, is_async=False, vars=()):
        """
        Add or replace this code at this location.
        """
        # test-compile the code for validity
        if code is None:
            return await self.remove(*path)

        make_proc(code, vars, *path, use_async=is_async)

        r = await self.client.set(*(self._path+path), value=dict(code=code,
            is_async=is_async, vars=vars), nchain=2)
        await self.wait_chain(r.chain)

    async def remove(self, *path):
        """Drop this code"""
        r = await self.client.set(*(self._path+path), value=None, nchain=2)
        await self.wait_chain(r.chain)

    def __call__(self, name, *a, **kw):
        c = self
        for k in name.split('.'):
            c = c[k]
        if c is self:
            raise RuntimeError("Empty code names don't make sense")
        return c(*a, **kw)


class ProcEntry(ClientEntry):
    _code = None

    @property
    def name(self):
        return '.'.join(self.subpath)

    async def set_value(self, value):
        await super().set_value(value)
        if value is None:
            self._code = None
            return

        try:
            v = self.value
            c = v['code']
            p = make_proc(c, v.get('vars',()), *self.subpath,
                    use_async=v.get('is_async', False))
            self._code = p
        except Exception as exc:
            logger.warning("Could not compile @%r", self.subpath)
            await self.root._err.record_exc("compile", *self.subpath,
                    exc=exc, reason="compilation", message="compiler error")
            self._code = None
        else:
            await self.root._err.record_working("compile", *self.subpath)

    def __call__(self, *a,**kw):
        if self._code is None:
            raise EmptyCode(self._path)
        return self._code(*a, **kw)


async def get_code_handler(client, cfg={}):
    """Return the code handler for this client.
    
    The handler is created if it doesn't exist.
    """
    c = {}
    c.update(CFG)
    c.update(cfg)
    def make():
        return client.mirror(*c['prefix'], root_type=ProcRoot,
                need_wait=True)

    return await client.unique_helper(*c['prefix'], factory=make)


