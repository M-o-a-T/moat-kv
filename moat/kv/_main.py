#!/usr/bin/env python3
"""
Basic DistKV support

"""

from moat.util import load_subgroup

@load_subgroup( name="distkv", sub_pre="distkv.command", sub_post="cli", prefix="distkv")
async def cli():
    """Various utilities"""
    pass  # pylint:disable=unnecessary-pass

