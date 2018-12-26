# Local server

import anyio
from anyio.exceptions import ClosedResourceError
import msgpack
from aioserf import serf_client

from .model import Entry, NodeEvent, Node
from .util import attrdict
from . import _version_tuple

import logging
logger = logging.getLogger(__name__)

_packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

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
    
    async def _task(self, proc, seq, args):
        async with anyio.open_cancel_scope() as s:
            self.tasks[seq] = s
            try:
                await self.send({'seq':seq, 'state':'start'})
                await proc(seq, *args)
            finally:
                del self.tasks[seq]
                await self.send({'seq':seq, 'state':'end'})

    async def task(self, proc, seq, *args):
        await self.tg.spawn(self._task, proc, seq, args)
        return _LATER

    async def _get_values(self, seq, entry):
        if entry.data is not None:  # TODO: send if recently deleted
            await self.send_result(seq, entry.serialize(chop_path=self._chop_path))
        for v in list(entry.values()):
            await self._get_values(seq, v)

    async def process(self, msg):
        if self.seq >= msg.seq:
            raise ValueError("Sequence numbers are not monotonic: %d < %d" % (self.seq, msg.seq))
        self.seq = msg.seq
        try:
            fn = getattr(self, 'cmd_' + str(msg.action))
        except AttributeError:
            raise ValueError("Command not recognized: " + repr(msg.action))
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
            entry = entry.serialize(chop_path=-1, depth=msg.get('depth', 0))
        return entry

    async def cmd_set_value(self, msg):
        """Set a node's value.
        """
        entry = self.root.follow(*msg.path)
        if entry.data != msg.value:
            await entry.set_data(self.server.event, msg.value)
        return entry.changes.serialize(depth=1)

    async def cmd_delete_value(self, msg):
        """Delete a node's value.
        Sub-nodes are not affected.
        """
        try:
            entry = self.root.follow(*msg.path)
        except KeyError:
            return False
        if entry.data is not None:
            await entry.set_data(self.server.event, None)
        return entry.changes.serialize(depth=2)

    async def cmd_get_tree(self, msg):
        try:
            entry = self.root.follow(*msg.path, create=False)
        except KeyError:
            return None
        if not len(entry):
            return entry.serialize(chop_path=self._chop_path)
        return await self.task(self._get_values, msg.seq, entry)

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
        return self.send({'seq': seq, 'result': res})

    async def run(self):
        unpacker = msgpack.Unpacker(object_pairs_hook=attrdict, raw=False, use_list=False)

        async with anyio.create_task_group() as tg:
            self.tg = tg
            await self.send({'seq': 0, 'version': _version_tuple,
                'node':self.server.node.name, 'local':self.server.node.tick})

            while True:
                for msg in unpacker:
                    seq = -1
                    try:
                        logger.debug("IN %d: %s", self._client_nr, msg)
                        seq = msg.seq
                        res = await self.process(msg)
                    except Exception as exc:
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
                                await self.send({'error': "Uncodeable", 'result': repr(res), 'seq': seq})

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

    async def monitor(self):
        async with self.serf.stream('user:distkv.*') as stream:
            async for resp in stream:
                print(resp)

    async def serve(self, cfg={}):
        async with serf_client(**cfg.serf) as serf:
            self.serf = serf
            self.spawn = serf.spawn

            await serf.spawn(self.monitor)
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

