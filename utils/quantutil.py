from datetime import date
from typing import *

import pandas as pd
import numpy as np


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


def fit(values: Any, scale: Tuple):
    # replace inf to max not inf
    values = pd.Series(values)
    values[values == np.inf] = values[values != np.inf].max()
    norm = (values - values.min()) / (values.max() - values.min())
    return scale[0] + norm * (scale[1] - scale[0])
