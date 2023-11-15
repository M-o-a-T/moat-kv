__path__ = __import__("pkgutil").extend_path(__path__, __name__)

try:
	from moat._dev_fix import _fix
except ImportError:
	pass
else:
	_fix()
