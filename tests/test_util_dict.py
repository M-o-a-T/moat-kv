# Some rudimentary tests for merge and combine_dict

from distkv.util import NotGiven, combine_dict, merge


def chkm(a, b, c, drop=False):
    r = merge(a, b, drop=drop)
    assert r == c


def chkc(a, b, c):
    r = combine_dict(a, b)
    assert r == c


def test_merge():
    chkm(dict(a=1, b=2, c=3), dict(b=NotGiven), dict(a=1, c=3))
    chkm(dict(a=1, b=2, c=3), dict(b=4, d=5), dict(a=1, b=4, c=3, d=5))
    chkm(dict(a=1, b=[1, 2, 3], c=3), dict(b=(4, NotGiven, None, 6)), dict(a=1, b=[4, 3, 6], c=3))
    chkm(
        dict(a=1, b=[1, 2, 3], c=3), dict(b={0: 4, 1: NotGiven, 3: 6}), dict(a=1, b=[4, 3, 6], c=3)
    )
    chkm(
        dict(a=1, b=[1, 2, 3], c=3),
        dict(a=1, b=(4, NotGiven, None, 6)),
        dict(a=1, b=[4, 3, 6]),
        drop=True,
    )


def test_combine():
    chkc(dict(a=1, b=2, c=3), dict(b=4, d=5), dict(a=1, b=2, c=3, d=5))
    chkc(dict(a=1, b=2, c=3), dict(b=NotGiven), dict(a=1, b=2, c=3))
    chkc(dict(b=NotGiven), dict(a=1, b=2, c=3), dict(a=1, c=3))
