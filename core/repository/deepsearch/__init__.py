import json
import logging
from datetime import date
from typing import *

import pandas
import pandas as pd
from requests import get

from config import config
from core.repository.mongo import DsCollection
from base.timeutil import YearQuarter

_logger = logging.getLogger(__file__)


def query(title: str, year: int, quarter: int = None):
    _logger.info(f"Requesting query - title={title}, year={year}, quarter={quarter}")

    today = date.today()
    last_confirmed = YearQuarter.last_confirmed(today.year, today.month)
    if quarter:
        assert last_confirmed >= YearQuarter(year, quarter)
        params = {'input': f'상장 기업 and {title} {year}년 {quarter}분기'}
    else:
        assert last_confirmed >= YearQuarter(year, 4)
        params = {'input': f'상장 기업 and {title} {year}'}

    response = get(
        'https://api.deepsearch.com/v1/compute',
        params=params,
        headers={'Authorization': config['deepSearchAuth']}
    )

    assert response.status_code == 200, f'Response code not 200, actual - {response.status_code}'
    content = json.loads(response.content)
    # noinspection DuplicatedCode
    assert content
    assert content["success"] is True
    data = content["data"]
    assert not data["exceptions"]
    subpod = data['pods'][0]['subpods'][0]
    assert subpod['class'] == 'Compiler:CompilationSucceeded'
    assert subpod['content']['data'][0] == 'Compilation succeeded.'
    return content


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
