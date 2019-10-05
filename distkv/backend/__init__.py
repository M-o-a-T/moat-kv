
def load_backend(name):
    from importlib import import_module
    if '.' not in name:
        name = "distkv.backend."+name
    return import_module(name).connect

