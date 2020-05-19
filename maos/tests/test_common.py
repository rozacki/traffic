from maos.common import *

def test_datetime_fromisoformat():
    d = fromisoformat("2018-01-01T00:14:59")
    assert d.year == 2018
    assert d.month == 1
    assert d.day == 1
    assert d.hour == 0
    assert d.minute == 14
    assert d.second == 59


def test_time_fromisoformat():
    t = fromisoformat('00:14:00')
    print(type(t))
    assert t.hour == 0
    assert t.minute == 14
    assert t.second == 0

