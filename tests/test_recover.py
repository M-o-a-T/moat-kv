import logging
import os

import mock
import pytest
import trio
from asyncactor.actor import Actor
from moat.util import P, Path

from moat.kv.mock.mqtt import stdtest
from moat.kv.server import Server

logger = logging.getLogger(__name__)

N = 10
NN = 10
NX = 10

rs = os.environ.get("PYTHONHASHSEED", None)
if rs is None:
    import random
else:
    import trio._core._run as tcr

    random = tcr._r

F1 = 0.05
F2 = 0.05
F3 = 0.05
F4 = 0.2

_asm = Actor._send_msg


async def send_msg(self, msg):
    if random.random() < F1:
        logger.info("NoMSG %s %r", self._name, msg)
        return
    logger.debug("MSG %s %r", self._name, msg)
    await _asm(self, msg)


_aqm = Actor.queue_msg


async def queue_msg(self, msg):
    if random.random() < F2:
        logger.info("NoQUM %s %r", self._name, msg)
        return
    # logger.debug("QUM %s %r",self._name, msg)
    await _aqm(self, msg)


_old_send = Server._send_event


async def send_evt(self, action: str, msg: dict):
    if random.random() < F3:
        logger.info("NoOUT %s %s %r", self.node.name, action, msg)
        return
    logger.debug("OUT %s %s %r", self.node.name, action, msg)
    return await _old_send(self, action, msg)


_old_upm = Server._unpack_multiple


def unpack_multiple(self, msg: dict):
    if random.random() < F4:
        logger.info("NoIN  %s %r", self.node.name, msg)
        return
    logger.debug("IN  %s %r", self.node.name, msg)
    return _old_upm(self, msg)


@pytest.mark.trio
async def test_10_recover(autojump_clock):  # pylint: disable=unused-argument
    """
    This test starts multiple servers at the same time and checks that
    dropping random messages ultimately recovers.
    """
    async with stdtest(test_1={"init": 420}, n=N, tocks=15000) as st:
        assert st is not None
        st.ex.enter_context(
            mock.patch("asyncactor.actor.Actor._send_msg", new=send_msg)
        )
        st.ex.enter_context(
            mock.patch("asyncactor.actor.Actor.queue_msg", new=queue_msg)
        )
        st.ex.enter_context(
            mock.patch("moat.kv.server.Server._send_event", new=send_evt)
        )
        st.ex.enter_context(
            mock.patch("moat.kv.server.Server._unpack_multiple", new=unpack_multiple)
        )

        for x in range(NX):
            for i in range(N):
                async with st.client((i + x) % N) as ci:
                    for j in range(NN):
                        await ci.set(Path("test", i, j), value=(i, j, x))

        await trio.sleep(1)

        missed = True
        while missed:
            missed = False
            for s in range(N):
                async with st.client(s) as ci:
                    c = 0
                    async for r in ci.get_tree(P("test"), min_depth=2, nchain=5):
                        if r.value == (r.path[-2], r.path[-1], NX - 1):
                            c += 1
                        else:
                            logger.info("%d: old %r", s, r)
                    if c != N * NN:
                        res = await ci._request("get_state", iter=False, missing=True)
                        logger.info("%d: missing %d %r", s, N * NN - c, res.missing)
                        missed = True
            if missed:
                await trio.sleep(100)
