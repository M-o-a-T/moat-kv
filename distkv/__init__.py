"""Top-level package for DistKV."""

try:
    import pkg_resources  # part of setuptools

    _version = pkg_resources.require("distkv")[0].version
    del pkg_resources

    _version_tuple = tuple(int(x) for x in _version.split("."))

except Exception:
    _version = "0.0.1"
    _version_tuple = (0, 0, 1)
