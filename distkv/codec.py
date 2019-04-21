# msgpack and co.

import msgpack
from functools import partial

from .util import attrdict

__all__ = ["packer", "unpacker", "stream_unpacker"]

# single message packer
packer = msgpack.Packer(strict_types=False, use_bin_type=True).pack

# single message unpacker
unpacker = partial(
    msgpack.unpackb, object_pairs_hook=attrdict, raw=False, use_list=False
)

# Data stream unpacker factory
stream_unpacker = lambda: msgpack.Unpacker(
    object_pairs_hook=attrdict, raw=False, use_list=False
)
