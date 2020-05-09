"""
This module contains helper functions for packing+unpacking of single messages,
plus an unpacker factory for streams.
"""

import msgpack
from functools import partial

from .util import attrdict

__all__ = ["packer", "unpacker", "stream_unpacker"]

def _encode(data):
    if isinstance(data,int) and data >= 1<<64:
        return msgpack.ExtType(2, data.to_bytes((data.bit_length()+7)//8, 'big'))
    return data

def _decode(code, data):
    if code == 2:
        return int.from_bytes(data, 'big')
    return msgpack.ExtType(code,data)

# single message packer
packer = msgpack.Packer(strict_types=False, use_bin_type=True, default=_encode).pack

# single message unpacker
unpacker = partial(
    msgpack.unpackb, object_pairs_hook=attrdict, raw=False, use_list=False, ext_hook=_decode
)

# stream unpacker factory
stream_unpacker = partial(msgpack.Unpacker,
    object_pairs_hook=attrdict, raw=False, use_list=False, ext_hook=_decode
)
