# pylint: disable=W0703,C0103

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

try:
    import pkg_resources  # part of setuptools

    _version = pkg_resources.require("moat.kv")[0].version
    del pkg_resources

    _version_tuple = tuple(int(x) for x in _version.split("."))

except Exception:  # pragma: no cover
    _version = "0.0.1"
    _version_tuple = (0, 0, 1)
