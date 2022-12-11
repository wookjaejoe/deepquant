from datetime import date
from typing import *

import pandas as pd


def power(series: pd.Series):
    norm = normalize(series)
    return norm / (norm.max() - norm.min())


def normalize(series: pd.Series, based_zero: bool = False) -> pd.Series:
    result = (series - series.mean()) / series.std()
    if based_zero:
        result = result + abs(min(result))

    return result


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


def rate_of_return(initial: float, final: float):
    return (final - initial) / initial


def N(target: str):
    return of("N", target)


def R(target: str):
    return of("R", target)


def of(operation: str, subject: str):
    return f"{operation}({subject})"
