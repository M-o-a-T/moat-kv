def combine_dict(*d):
    res = {}
    keys = {}
    if len(d) <= 1:
        return d
    for kv in d:
        for k,v in kv.items():
            if k not in keys:
                keys[k] = []
            keys[k].append(v)
    for k,v in keys.items():
        if len(v) == 1:
            res[k] = v[0]
        elif not isinstance(v[0],Mapping):
            for vv in v[1:]:
                assert not isinstance(vv,Mapping)
            res[k] = v[0]
        else:
            res[k] = combine_dict(*v)
    return res

class attrdict(dict):
    """A dictionary which can be accessed via attributes, for convenience"""
    def __init__(self,*a,**k):
        super(attrdict,self).__init__(*a,**k)
        self._done = set()

    def __getattr__(self,a):
        if a.startswith('_'):
            return super(attrdict,self).__getattr__(a)
        try:
            return self[a]
        except KeyError:
            raise AttributeError(a)
    def __setattr__(self,a,b):
        if a.startswith("_"):
            super(attrdict,self).__setattr__(a,b)
        else:
            self[a]=b
    def __delattr__(self,a):
        del self[a]

import yaml
from yaml.representer import SafeRepresenter
SafeRepresenter.add_representer(attrdict, SafeRepresenter.represent_dict)

