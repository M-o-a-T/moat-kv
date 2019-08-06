# command line interface

import os
import sys
import asyncclick as click
import yaml

from distkv.util import yprint

import logging
logger = logging.getLogger(__name__)


async def data_get(obj, path, recursive=True, as_dict='_', maxdepth=-1, mindepth=0, empty=False, raw=False):
    if recursive:
        if raw:
            raise click.UsageError("'raw' cannot be used with 'recursive'")

        kw = {}
        if maxdepth is not None:
            kw["max_depth"] = maxdepth
        if mindepth is not None:
            kw["min_depth"] = mindepth
        if empty:
            kw["add_empty"] = True
        y = {}
        async for r in obj.client.get_tree(*path, nchain=obj.meta, **kw):
            r.pop("seq", None)
            path = r.pop("path")
            if as_dict is not None:
                yy = y
                for p in path:
                    yy = yy.setdefault(p, {})
                try:
                    yy[as_dict] = r if obj.meta else r.value
                except AttributeError:
                    continue
            else:
                y = {}
                try:
                    y[path] = r if obj.meta else r.value
                except AttributeError:
                    continue
                yprint([y], stream=obj.stdout)

        if as_dict is not None:
            yprint(y, stream=obj.stdout)
        return

    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    if as_dict is not None:
        raise click.UsageError("'as-dict' only works with 'recursive'")
    res = await obj.client.get(*path, nchain=obj.meta)
    if not obj.meta:
        try:
            res = res.value
        except AttributeError:
            if obj.debug:
                print("No data at", repr(path), file=sys.stderr)
            sys.exit(1)

    if not raw:
        yprint(res, stream=obj.stdout)
    elif isinstance(res, bytes):
        os.write(obj.stdout.fileno(), res)
    else:
        obj.stdout.write(str(res))


