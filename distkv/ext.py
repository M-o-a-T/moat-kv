"""
Code to load external modules
"""

import os
import asyncclick as click


def load_one(name, path, endpoint=None, **ns):
    fn = os.path.join(path, name) + ".py"
    if not os.path.isfile(fn):
        fn = os.path.join(path, name, "__init__.py")
    with open(fn) as f:
        ns["__file__"] = fn

        code = compile(f.read(), fn, "exec")
        eval(code, ns, ns)  # pylint: disable=eval-used
    if endpoint is not None:
        ns = ns[endpoint]
    return ns


def _namespaces():
    import pkgutil
    import distkv_ext as ext

    return pkgutil.iter_modules(ext.__path__, ext.__name__ + ".")


_ext_cache = {}


def _cache_ext():
    """List external modules

    Yields (name,path) tuples.

    TODO: This is not zip safe.
    """
    for finder, name, ispkg in _namespaces():
        if not ispkg:
            continue
        x = name.rsplit(".", 1)[-1]
        f = os.path.join(finder.path, x)
        _ext_cache[x] = f


def list_ext(func=None):
    """List external modules

    Yields (name,path) tuples.

    TODO: This is not zip safe.
    """
    if not _ext_cache:
        _cache_ext()
    if func is None:
        yield from iter(_ext_cache.items())
        return
    for x, f in _ext_cache.items():
        fn = os.path.join(f, func) + ".py"
        if not os.path.exists(fn):
            fn = os.path.join(f, func, "__init__.py")
            if not os.path.exists(fn):
                continue
        yield (x, f)


def load_ext(name, func, endpoint=None, **kw):
    """
    Load an external module.

    Example: ``load_ext("owfs","model")`` loads …/distkw_ext/owfs/model.py
    and returns its global dict. When "ep" is given it returns the entry
    point.

    Any additional keywords are added to the module dictionary.

    TODO: This is not zip safe. It also doesn't return a proper module.
    Don't use this with modules that are also loaded the regular way.
    """

    if not _ext_cache:
        _cache_ext()

    try:
        return load_one(func, _ext_cache[name], endpoint, **kw)
    except KeyError:
        raise click.UsageError(f"I do not know {name !r}")
