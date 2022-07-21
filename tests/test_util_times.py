# Tests to figure out how long until a time spec fits / does not fit

from distkv.util.times import time_until
import datetime
import sys

lnp = None
err = False
now = None


def chk(iso, a, invert=False):
    if a == "":
        a = ()
    else:
        a = a.split(" ")
    res = time_until(a, t_now=now, invert=invert)
    if res is None:
        if iso == "-":
            return
    res -= datetime.timedelta(0, 0, res.microsecond)
    res = str(res)
    if iso == res:
        return

    global err
    global lnp
    if lnp is None or lnp != now:
        lnp = now
        print("@", now, "::")
    err += 1
    print("?", iso, "≠", res, "@", a)


def test_all():
    global now
    now = datetime.datetime(2003, 4, 5, 6, 7, 8)

    chk("2003-04-05 06:07:08", "6 h 4 month 8 sec")
    chk("2003-04-05 06:07:10", "10 sec")
    chk("2003-04-05 06:08:02", "2 sec")
    chk("2003-04-05 06:07:50", "- 10 sec")
    chk("2003-04-05 06:11:02", "11 min 2 sec")
    chk("2003-04-05 11:00:50", "11 h - 10 sec")
    chk("2003-04-05 23:05:50", "- 1 h 5 min - 10 sec")

    chk("2003-12-29 00:00:00", "1 wk")
    chk("2004-01-01 00:00:12", "1 wk thu 12 sec")
    chk("2004-01-05 00:00:00", "2 wk")

    chk("2003-04-05 11:00:50", "11 h - 10 sec")
    chk("2003-04-05 11:45:50", "11 h - 15 min - 10 sec")
    chk("2003-04-05 06:07:08", "14 wk")
    chk("2003-04-07 00:00:00", "15 wk")
    chk("2004-03-22 00:00:00", "13 wk")

    #       April                  May
    # Su Mo Tu We Th Fr Sa  Su Mo Tu We Th Fr Sa
    #       1  2  3  4  5               1  2  3
    # 6  7  8  9 10 11 12   4  5  6  7  8  9 10
    # 13 14 15 16 17 18 19  11 12 13 14 15 16 17
    # 20 21 22 23 24 25 26  18 19 20 21 22 23 24
    # 27 28 29 30           25 26 27 28 29 30 31

    chk("2003-04-05 06:07:08", "1 sat")
    chk("2003-04-08 00:00:00", "2 tue")
    chk("2003-04-16 00:00:00", "3 wed")
    chk("2003-04-16 00:00:00", "-3 wed")
    chk("2003-04-24 00:00:00", "-1 thu")
    chk("2003-05-01 00:00:00", "1 thu")
    chk("2003-05-07 00:00:00", "1 wed")

    chk("-", "", True)
    chk("2003-04-05 06:07:09", "8 sec", True)
    chk("2003-04-05 06:07:08", "9 sec", True)
    chk("2003-04-05 06:07:09", "7 min 8 sec", True)
    chk("2003-04-05 06:07:08", "7 min 0 sec", True)

    chk("2003-04-07 00:00:00", "14 wk", True)
    chk("2003-04-05 06:07:08", "14 wk mon", True)
    chk("2003-04-06 00:00:00", "14 wk sat", True)
    chk("2003-04-05 06:07:08", "15 wk", True)

    if err:
        sys.exit(1)
