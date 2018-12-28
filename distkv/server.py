# Local server

import anyio
from anyio.exceptions import ClosedResourceError
import msgpack
from aioserf import serf_client

from .model import Entry, NodeEvent, Node, Watcher
from .util import attrdict
from . import _version_tuple

import logging
logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

class ClientError(ValueError):
    """report error to the client but don't dump a stack trace"""
    pass

class _LATER:
    pass

_client_nr = 0


def _result(seq, res):
    return {'seq': seq, 'result': res}

class ServerClient:
    """Represent one listening client"""
    _nursery = None

    def __init__(self, server, root, stream):
        self.server = server
        self.root = root
        self.stream = stream
        self.seq = 0
        self.tasks = {}
        self._chop_path = 0

        global _client_nr
        _client_nr += 1
        self._client_nr = _client_nr
        logger.debug("CONNECT %d %s", self._client_nr, repr(stream))
    
    async def _task(self, proc, seq, args, kwargs):
        async with anyio.open_cancel_scope() as s:
            self.tasks[seq] = s
            try:
                await self.send({'seq':seq, 'state':'start'})
                await proc(seq, *args, **kwargs)
            finally:
                del self.tasks[seq]
                await self.send({'seq':seq, 'state':'end'})

    async def task(self, proc, seq, *args, **kwargs):
        await self.tg.spawn(self._task, proc, seq, args, kwargs)
        return _LATER

    async def _get_values(self, seq, entry, nchain=0, depth=None, cdepth=0):
        if entry.data is not None:  # TODO: send if recently deleted
            res = entry.serialize(chop_path=self._chop_path, nchain=nchain)
            if depth is not None:
                p = res['path'][depth+cdepth:]
                if len(p):
                    res['path'] = p
                else:
                    del res['path']
                res['depth'] = cdepth
                cdepth +=len(p)
            await self.send_result(seq, res)
        for v in list(entry.values()):
            await self._get_values(seq, v, nchain=nchain, depth=depth, cdepth=cdepth)

    async def _watch(self, seq, entry, nchain=0, depth=None):
        async with Watcher(entry) as watcher:
            cdepth = 0
            async for msg in watcher:
                res = msg.entry.serialize(chop_path=self._chop_path, nchain=nchain)
                if depth is not None:
                    p = res['path'][depth:]
                    nn=0
                    for n in range(len(p)):
                        nn = n
                        if n >= cdepth:
                            break
                        if p[n] != path[depth+n]:
                            break
                    res['depth'] = nn
                    p = p[nn:]
                    cdepth +=len(p)
                    if len(p):
                        res['path'] = p
                    else:
                        del res['path']
                await self.send_result(seq, res)

    async def process(self, msg):
        if self.seq >= msg.seq:
            raise ClientError("Sequence numbers are not monotonic: %d < %d" % (self.seq, msg.seq))
        self.seq = msg.seq
        try:
            fn = getattr(self, 'cmd_' + str(msg.action))
        except AttributeError:
            raise ClientError("Command not recognized: " + repr(msg.action))
        else:
            return await fn(msg)
        
    async def cmd_root(self, msg):
        """Change to a sub-tree.
        """
        entry = self.root.follow(*msg.path)
        self.root = entry
        self._chop_path += len(msg.path)
        return entry.serialize(chop_path=self._chop_path)

    async def cmd_get_value(self, msg):
        """Get a node's value.
        """
        try:
            entry = self.root.follow(*msg.path, create=False)
        except KeyError:
            entry = None
        else:
            entry = entry.serialize(chop_path=-1, nchain=msg.get('nchain', 0))
        return entry

    async def cmd_set_value(self, msg):
        """Set a node's value.
        """
        entry = self.root.follow(*msg.path)
        send_prev = True
        if 'prev' in msg:
            if entry.data != msg.prev:
                raise ClientError("Data is %s" % (repr(entry.data),))
            send_prev = False
        if 'chain' in msg:
            if msg.chain is None and entry.chain is not None:
                raise ClientError("This entry already exists")
            if msg.chain is not None and entry.chain is None:
                raise ClientError("This entry is new")
            if entry.chain is not None and entry.chain != msg.chain:
                raise ClientError("Chain is %s" % (repr(entry.chain),))
            send_prev = False
        res = {'changed': entry.data != msg.value}
        if send_prev:
            res['prev'] = entry.data

        await entry.set_data(self.server.event, msg.value)

        nchain = msg.get('nchain', 1)
        if nchain > 0:
            res['chain'] = entry.chain.serialize(nchain=nchain)
        return res

    async def cmd_delete_value(self, msg):
        """Delete a node's value.
        Sub-nodes are not affected.
        """
        if 'value' in msg:
            raise ClientError("A deleted entry can't have a value")
        msg.value = None
        return await self.cmd_set_value(msg)

    async def cmd_get_tree(self, msg):
        try:
            entry = self.root.follow(*msg.path, create=False)
        except KeyError:
            return {'value':None}
        if not len(entry):
            return entry.serialize(chop_path=self._chop_path)
        return await self.task(self._get_values, msg.seq, entry, nchain=msg.get('nchain',0), depth=len(msg.path))

    async def cmd_watch(self, msg):
        entry = self.root.follow(*msg.path, create=True)
        return await self.task(self._watch, msg.seq, entry, nchain=msg.get('nchain',0), depth=len(msg.path))

    async def cmd_delete_tree(self, msg):
        """Delete a node's value.
        Sub-nodes are also cleared.
        """
        try:
            entry = self.root.follow(*msg.path)
        except KeyError:
            return False
        if entry.data is not None:
            await entry.set_data(evt, self.server.event, None)
        return entry.timestamp

    async def cmd_stop(self, msg):
        try:
            t = self.tasks[msg.task]
        except KeyError:
            return False
        t.cancel()
        return True

    def send(self, msg):
        logger.debug("OUT%d: %s", self._client_nr, msg)
        return self.stream.send_all(_packer(msg))

    def send_result(self, seq, res):
        res['seq'] = seq
        return self.send(res)

    async def run(self):
        unpacker = msgpack.Unpacker(object_pairs_hook=attrdict, raw=False, use_list=False)

        async with anyio.create_task_group() as tg:
            self.tg = tg
            await self.send({'seq': 0, 'version': _version_tuple,
                'node':self.server.node.name, 'local':self.server.node.tick})

            while True:
                for msg in unpacker:
                    seq = 0
                    try:
                        logger.debug("IN %d: %s", self._client_nr, msg)
                        if 'chain' in msg and not isinstance(msg.chain, int):
                            msg.chain = NodeEvent.unserialize(msg.chain)
                        seq = msg.seq
                        res = await self.process(msg)
                    except Exception as exc:
                        if not isinstance(exc, ClientError):
                            logger.exception("Client error on %s", repr(msg))
                        if seq > 0:
                            await self.send({'error': str(exc), 'seq': seq})
                        else:
                            await self.send({'error': str(exc)})
                            return
                    else:
                        if res is not _LATER:
                            try:
                                await self.send_result(seq, res)
                            except Exception as exc:
                                try:
                                    await self.send({'error': "Uncodeable", 'result': repr(res), 'seq': seq})
                                except Exception as exc:
                                    await self.send({'error': "Uncodeable", 'result': None, 'seq': seq})

                try:
                    buf = await self.stream.receive_some(4096)
                except (ConnectionResetError, ClosedResourceError):
                    return  # closed/reset/whatever
                if len(buf) == 0:  # Connection was closed.
                    return # done
                unpacker.feed(buf)


class Server:
    serf = None

    def __init__(self, name: str, root: Entry, host: str, port: int):
        self.root = root
        self.host = host
        self.port = port
        self.node = Node(name, 1)

        self._nodes = {self.node.name: self.node}

    @property
    def event(self):
        self.node.tick += 1
        return NodeEvent(self.node)

    async def watcher(self):
        async with Watcher(self.root) as watcher:
            async for msg in watcher:
                p = msg.serialize(nchain=9)
                p = _packer(p)
                await self.serf.event('distkv.update', p, coalesce=False)

    async def monitor(self):
        async with self.serf.stream('user:distkv.update') as stream:
            async for resp in stream:
                msg = msgpack.unpackb(resp.payload, object_pairs_hook=attrdict, raw=False, use_list=False)
                print("UPDATE:",resp, msg)

    async def serve(self, cfg={}):
        async with serf_client(**cfg.serf) as serf:
            self.serf = serf
            self.spawn = serf.spawn

            await serf.spawn(self.monitor)
            await serf.spawn(self.watcher)
            cfg_s = cfg.server
            if 'host' in cfg_s:
                cfg_s = cfg_s.copy()
                cfg_s['interface'] = cfg_s.pop('host')
            async with await anyio.create_tcp_server(**cfg_s) as server:
                async for client in server.accept_connections():
                    await serf.spawn(self._connect, client)

    async def _connect(self, stream):
        c = ServerClient(server=self, root=self.root, stream=stream)
        await c.run()

