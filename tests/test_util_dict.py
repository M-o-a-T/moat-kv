# Some rudimentary tests for merge and combine_dict

from distkv.util import merge, combine_dict, NotGiven


def chkm(a, b, c):
    r = merge(a, b)
    assert r == c


def chkc(a, b, c):
    r = combine_dict(a, b)
    assert r == c


def test_merge():
    global now
    chkm(dict(a=1, b=2, c=3), dict(b=NotGiven), dict(a=1, c=3))
    chkm(dict(a=1, b=2, c=3), dict(b=4, d=5), dict(a=1, b=4, c=3, d=5))
    chkm(dict(a=1, b=[1, 2, 3], c=3), dict(b=(4, NotGiven, None, 6)), dict(a=1, b=[4, 3, 6], c=3))
    chkm(
        dict(a=1, b=[1, 2, 3], c=3), dict(b={0: 4, 1: NotGiven, 3: 6}), dict(a=1, b=[4, 3, 6], c=3)
    )


def test_combine():
    global now
    chkc(dict(a=1, b=2, c=3), dict(b=4, d=5), dict(a=1, b=2, c=3, d=5))
    chkc(dict(a=1, b=2, c=3), dict(b=NotGiven), dict(a=1, b=2, c=3))
    chkc(dict(b=NotGiven), dict(a=1, b=2, c=3), dict(a=1, c=3))
