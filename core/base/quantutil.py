from datetime import date
from typing import *


def xox(x1, x2, y=None):
    return (x2 - x1) / (y if y else abs(x1))


def cagr(initial: float, last: float, years: float):
    return (last / initial) ** (1 / years) - 1


def mdd(dates: List[date], values: List[float]):
    assert len(dates) == len(values)

    max_value = values[0]
    max_date = dates[0]

    _mdd = 0
    _mdd_section = ()
    for i in range(len(dates)):
        _date = dates[i]
        value = values[i]
        if value > max_value:
            max_value = value
            max_date = _date

        dd = value / max_value - 1
        if dd < _mdd:
            _mdd = dd
            _mdd_section = (max_date, _date)

    return _mdd, _mdd_section
