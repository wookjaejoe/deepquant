import json
import logging
from datetime import date
from typing import *

import pandas
from requests import get

from config import config
from repository.mongo import DsCollection
from base import Quarter

_logger = logging.getLogger(__file__)


def query(title: str, year: int, quarter: int = None):
    _logger.info(f"Requesting query - title={title}, year={year}, quarter={quarter}")

    today = date.today()
    last_confirmed = Quarter.last_confirmed(today.year, today.month)
    if quarter:
        assert last_confirmed >= Quarter(year, quarter)
        params = {'input': f'상장 기업 and {title} {year}년 {quarter}분기'}
    else:
        assert last_confirmed >= Quarter(year, 4)
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


def _load(raw: dict, title: str) -> pandas.DataFrame:
    content = raw['data']['pods'][1]['content']
    data = content['data']
    columns = content['columns']
    col = [col for col in columns if title in col][0]
    # 코드 normalization
    df = pandas.DataFrame({title: data[col]}, index=data['symbol'])
    df.index = [symbol.split(":")[1] for symbol in df.index]
    return df


def load_by_year(title: str, year: int) -> pandas.DataFrame:
    return _load(DsCollection.fetch_one(title, year), title)


def load_by_quart(title: str, year: int, quarter: int) -> pandas.DataFrame:
    return _load(DsCollection.fetch_one(title, year, quarter), title)


def load_many(title: str, year: int, month: int, num: int) -> Iterator[pandas.DataFrame]:
    return [load_by_quart(title, q.year, q.quarter) for q in Quarter.last_confirmed(year, month).iter_back(num)]


def load_and(title: str, year: int, month: int, num: int,
             operator: Callable[Iterator[pandas.DataFrame], pandas.DataFrame]) -> pandas.DataFrame:
    return operator(load_many(title, year, month, num))


def load_one(title: str, year: int, month: int) -> pandas.DataFrame:
    q = Quarter.last_confirmed(year, month)
    return load_by_quart(title, q.year, q.quarter)


def validate():
    # fixme: 분기 재무데이터가 정상적인지를 당기 재무데이터로 검증
    pass
