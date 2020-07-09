"""
This module contains helper functions for packing+unpacking of single messages,
plus an unpacker factory for streams.
"""

import msgpack
from functools import partial

from .util import attrdict, Path

__all__ = ["packer", "unpacker", "stream_unpacker"]


def _encode(data):
    if isinstance(data, int) and data >= 1 << 64:
        return msgpack.ExtType(2, data.to_bytes((data.bit_length() + 7) // 8, "big"))
    elif isinstance(data, Path):
        return msgpack.ExtType(3, b"".join(packer(x) for x in data))
    return data


def _decode(code, data):
    if code == 2:
        return int.from_bytes(data, "big")
    elif code == 3:
        s = stream_unpacker()
        s.feed(data)
        return Path(*s)
    return msgpack.ExtType(code, data)


# single message packer
_packers = []


def packer(data):
    if _packers:
        pack = _packers.pop()
    else:
        pack = msgpack.Packer(strict_types=False, use_bin_type=True, default=_encode).pack
    try:
        return pack(data)
    finally:
        _packers.append(pack)


# single message unpacker
unpacker = partial(
    msgpack.unpackb, object_pairs_hook=attrdict, raw=False, use_list=False, ext_hook=_decode
)

# stream unpacker factory
stream_unpacker = partial(
    msgpack.Unpacker, object_pairs_hook=attrdict, raw=False, use_list=False, ext_hook=_decode
)
