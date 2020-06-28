import pytest
from distkv.util import Path, yformat, yload
from distkv.codec import packer,unpacker

_valid = (
        (("a","b","c"),"a.b.c"),
        (("a",2,"c"),"a:2.c"),
        ((2,"c"),":2.c"),
        ((True,"c"),":t.c"),
        ((1.23,"c"),":1:.23.c"),
        (('',1.23,"c"),":e:1:.23.c"),
        (("a",'',1.23,"c"),"a:e:1:.23.c"),
        (("a",'',1.23),"a:e:1:.23"),
        (("a",'',"b"),"a:e.b"),
        (("a",True),"a:t"),
        (("x",None),"x:n"),
        (((1,2),1.23),":(1,2):1:.23"),
        (((1,2),'',1.23),":(1,2):e:1:.23"),
        (((1,2),"c"),":(1,2).c"),
        ((),":"),
    )

_invalid = (
        ":y",
        ":t:",
        "a.b:",
        ":2..c",
        "a..b",
        "a.:1",
        "a.:t",
        ".a.b",
        "a.b.",
        "",
        ":list",
        ":dict",
    )

@pytest.mark.parametrize("a,b",_valid)
def test_valid_paths(a,b):
    assert str(Path(*a)) == b
    assert a == tuple(Path.from_str(b))

@pytest.mark.parametrize("a",_invalid)
def test_invalid_paths(a):
    with pytest.raises(SyntaxError):
        Path.from_str(a)

def test_msgpack():
    d = ("a",1,"b")
    m = packer(d)
    mm = unpacker(m)
    assert type(mm) is tuple
    assert mm == d

    d = Path("a",1,"b")
    m = packer(d)
    mm = unpacker(m)
    assert type(mm) is Path
    assert mm == d

    d = {"Hello":d}
    m = packer(d)
    mm = unpacker(m)
    assert type(mm['Hello']) is Path
    assert mm == d

def test_yaml():
    a = Path.from_str("a.b.c")
    b = "!P a.b.c\n...\n"
    assert yformat(a) == b
    assert yload(b) == a
