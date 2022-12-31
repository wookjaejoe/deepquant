import logging
from typing import *

import pandas
import pandas as pd

from base.timeutil import YearQuarter
from core.repository.mongo import DsCollection

_logger = logging.getLogger(__file__)


def _load(raw: dict, title: str) -> pandas.Series:
    content = raw['data']['pods'][1]['content']
    data = content['data']
    columns = content['columns']
    col = [col for col in columns if title in col][0]
    # 코드 normalization
    result = pandas.Series(
        data[col],
        index=[symbol.split(":")[1] for symbol in data['symbol']],
        name=title
    )
    return result


def load_by_year(title: str, year: int) -> pandas.Series:
    return _load(DsCollection.fetch_one(title, year), title)


def load_by_quart(title: str, year: int, quarter: int) -> pandas.Series:
    return _load(DsCollection.fetch_one(title, year, quarter), title)


def load_many(title: str, year: int, month: int, num: int) -> Iterator[pandas.Series]:
    last = YearQuarter.last_confirmed(year, month)
    return [load_by_quart(title, yq.year, yq.quarter) for yq in [last.minus(i) for i in range(num)]]


def load_and_sum(title: str, year: int, month: int, num: int) -> pd.Series:
    df = pd.DataFrame()
    count = 0
    for one in load_many(title, year, month, num):
        df = df.merge(one.rename(f"{one.name}_{count}"), how="outer", left_index=True, right_index=True)
        count += 1

    # fixme: na를 그냥 drop 시키면 종목 누락인데, fillna 합리적으로 할 수 있는 방법을 찾아봐야 함.
    result = df.dropna().sum(axis=1)
    result.name = title
    return result


def load_and(
    title: str, year: int, month: int, num: int,
    operator: Callable[[Iterator[pandas.Series]], pandas.Series],
) -> pandas.Series:
    return operator(load_many(title, year, month, num))


def load_one(title: str, year: int, month: int) -> pandas.Series:
    yq = YearQuarter.last_confirmed(year, month)
    return load_by_quart(title, yq.year, yq.quarter)
