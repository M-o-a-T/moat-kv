"""
Data access
"""
import sys
import os
import time
import datetime
import asyncclick as click
from collections.abc import Mapping

from distkv.util import yprint, Path, NotGiven, attrdict


def add_dates(d):
    """
    Given a dict with int/float entries that might conceivably be dates,
    add ``_*`` with a textual representation.
    """

    t = time.time()
    start = t - 366 * 24 * 3600
    stop = t + 366 * 24 * 3600

    def _add(d):
        if isinstance(d, (list, tuple)):
            for dd in d:
                _add(dd)
            return
        if not isinstance(d, Mapping):
            return
        for k, v in list(d.items()):
            if k.startswith("_"):
                continue
            if not isinstance(v, (int, float)):
                _add(v)
                continue
            if start <= v <= stop:
                d[f"_{k}"] = datetime.datetime.fromtimestamp(v).isoformat(
                    sep=" ", timespec="milliseconds"
                )

    _add(d)


async def data_get(
    obj,
    path,
    *,
    recursive=True,
    as_dict="_",
    maxdepth=-1,
    mindepth=0,
    empty=False,
    raw=False,
    internal=False,
    path_mangle=None,
    item_mangle=None,
    add_date=False,
):
    """Generic code to dump a subtree.

    `path_mangle` accepts a path and the as_dict parameter. It should
    return the new path. This is used for e.g. prefixing the path with a
    device name. Returning ``None`` causes the entry to be skipped.
    """
    if path_mangle is None:
        path_mangle = lambda x: x
    if item_mangle is None:

        async def item_mangle(x):  # pylint: disable=function-redefined
            return x

    if recursive:
        kw = {}
        if maxdepth is not None:
            kw["max_depth"] = maxdepth
        if mindepth is not None:
            kw["min_depth"] = mindepth
        if empty:
            kw["empty"] = True
        if obj.meta:
            kw.setdefault("nchain", obj.meta)
        y = {}
        if internal:
            res = await obj.client._request(action="get_tree_internal", path=path, iter=True, **kw)
        else:
            res = obj.client.get_tree(path, nchain=obj.meta, **kw)
        async for r in res:
            r = await item_mangle(r)
            if r is None:
                continue
            r.pop("seq", None)
            path = r.pop("path")
            path = path_mangle(path)
            if path is None:
                continue
            if add_date and "value" in r:
                add_dates(r.value)

            if as_dict is not None:
                yy = y
                for p in path:
                    yy = yy.setdefault(p, {})
                try:
                    yy[as_dict] = r if obj.meta else r.value
                except AttributeError:
                    if empty:
                        yy[as_dict] = None
            else:
                if raw:
                    y = path
                else:
                    y = {}
                    try:
                        y[path] = r if obj.meta else r.value
                    except AttributeError:
                        if empty:
                            y[path] = None
                        else:
                            continue
                yprint([y], stream=obj.stdout)

        if as_dict is not None:
            if maxdepth:

                def simplex(d):
                    for k, v in d.items():
                        if isinstance(v, dict):
                            d[k] = simplex(d[k])
                    if as_dict in d and d[as_dict] is None:
                        if len(d) == 1:
                            return None
                        else:
                            del d[as_dict]
                    return d

                y = simplex(y)
            yprint(y, stream=obj.stdout)
        return

    if maxdepth is not None or mindepth is not None:
        raise click.UsageError("'mindepth' and 'maxdepth' only work with 'recursive'")
    if as_dict is not None:
        raise click.UsageError("'as-dict' only works with 'recursive'")
    res = await obj.client.get(path, nchain=obj.meta)
    if not obj.meta:
        try:
            res = res.value
        except AttributeError:
            if obj.debug:
                print("No data at", path, file=sys.stderr)
            sys.exit(1)

    if not raw:
        yprint(res, stream=obj.stdout)
    elif isinstance(res, bytes):
        os.write(obj.stdout.fileno(), res)
    else:
        obj.stdout.write(str(res))


def res_get(res, attr: Path, **kw):  # pylint: disable=redefined-outer-name
    """
    Get a node's value and access the dict items beneath it.

    The node value must be an attrdict.
    """
    val = res.get("value", None)
    if val is None:
        return None
    return val._get(attr, **kw)


def res_update(res, attr: Path, value=None, **kw):  # pylint: disable=redefined-outer-name
    """
    Set a node's sub-item's value, possibly merging dicts.
    Entries set to 'NotGiven' are deleted.

    The node value must be an attrdict.

    Returns the new value.
    """
    val = res.get("value", attrdict())
    return val._update(attr, value=value, **kw)


def res_delete(res, attr: Path, **kw):  # pylint: disable=redefined-outer-name
    """
    Remove a node's sub-item's value, possibly removing now-empty
    intermediate dicts.

    The node value must be an attrdict.

    Returns the new value.
    """
    val = res.get("value", attrdict())
    return val._delete(attr, **kw)


async def node_attr(
    obj, path, attr, value=NotGiven, eval_=False, split_=False, res=None, chain=None
):
    """
    Sub-attr setter.

    Args:
        obj: command object
        path: address of the node to change
        attr: path of the element to change
        value: new value (default NotGiven)
        eval_: evaluate the new value? (default False)
        split_: split a string value into words? (bool or separator, default False)
        res: old node, if it has been read already
        chain: change chain of node, copied from res if clear

    Special: if eval_ is True, a value of NotGiven deletes, otherwise it
    prints the record without changing it. A mapping replaces instead of updating.

    Returns the result of setting the attribute, or ``None`` if it printed
    """
    if res is None:
        res = await obj.client.get(path, nchain=obj.meta or 2)
    if chain is None:
        chain = res.chain

    try:
        val = res.value
    except AttributeError:
        chain = None
    if split_ is True:
        split_ = ""
    if eval_:
        if value is NotGiven:
            value = res_delete(res, attr)
        else:
            value = eval(value)  # pylint: disable=eval-used
            if split_ is not False:
                value = value.split(split_)
            value = res_update(res, attr, value=value)
    else:
        if value is NotGiven:
            if not attr and obj.meta:
                val = res
            else:
                val = res_get(res, attr)
            yprint(val, stream=obj.stdout)
            return
        if split_ is not False:
            value = value.split(split_)
        value = res_update(res, attr, value=value)

    res = await obj.client.set(path, value=value, nchain=obj.meta, chain=chain)
    return res
