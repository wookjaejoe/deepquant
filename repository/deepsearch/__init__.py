import json
import os

import pandas
from requests import get, Response
from resources import pre_queried_dir
from config import config
from typing import *
from base import Quarter


def collect_by_quart(title: str, year: int, quart: int):
    response = get(
        'https://api.deepsearch.com/v1/compute',
        params={
            'input': f'상장 기업 and {title} {year}년 {quart}분기'
        },
        headers={
            'Authorization': config['deepSearchAuth']
        }
    )

    check_successful(response)

    folder = os.path.join(pre_queried_dir, title)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f'{year}-{quart}.json')
    print(f"Writing query result in {path}")
    with open(path, 'wb') as f:
        f.write(response.content)


def collect_by_year(title: str, year):
    response = get(
        'https://api.deepsearch.com/v1/compute',
        params={
            'input': f'상장 기업 and {title} {year}'
        },
        headers={
            'Authorization': config['deepSearchAuth']
        }
    )

    check_successful(response)

    folder = os.path.join(pre_queried_dir, title)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f'{year}.json')
    print(f"Writing query result in {path}")
    with open(path, 'wb') as f:
        f.write(response.content)


def check_successful(response: Response):
    assert response.status_code == 200, f'Response code not 200, actual - {response.status_code}'
    query_result = json.loads(response.content)
    subpod = query_result['data']['pods'][0]['subpods'][0]
    fail_message = 'Query result has a failure'
    assert subpod['class'] == 'Compiler:CompilationSucceeded', fail_message
    assert subpod['content']['data'][0] == 'Compilation succeeded.', fail_message


def _load(file: str, title: str):
    with open(file) as f:
        d = json.load(f)

    content = d['data']['pods'][1]['content']
    data = content['data']
    columns = content['columns']
    rows = {title: data[[col for col in columns if title in col][0]]}

    return pandas.DataFrame(rows, index=data['symbol'])


def load_by_year(title: str, year: int):
    return _load(os.path.join(pre_queried_dir, title, f'{year}.json'), title)


def load_by_quart(title: str, year: int, quart: int):
    return _load(os.path.join(pre_queried_dir, title, f'{year}-{quart}.json'), title)


# 1,2 - 작년3분기(11월 15일까지 발표), 작년2분기, 작년1분기, 제작년4분기
# 3,4 - 작년 당기데이터 조회
# 5,6,7 - 1분기(5월 15일까지 발표), 작년4분기, 작년3분기, 작년2분기 <- 이때도 당기 데이터 조회하면 코넥스 커버 가능
# 8,9,10 - 2분기(8월 15일까지 발표), 1분기, 작년4분기, 작년3분기
# 11,12 - 3분기(11월 15일까지 발표), 2분기, 1분기, 작년1분기
def load(title: str, year: int, month: int, num: int) -> Iterator[pandas.DataFrame]:
    if month in [1, 2]:
        return [load_by_quart(title, q.year, q.quarter) for q in Quarter(year - 1, 3).iter_back(num)]
    elif month in [3, 4]:
        return [load_by_quart(title, q.year, q.quarter) for q in Quarter(year - 1, 4).iter_back(num)]
    elif month in [5, 6, 7]:
        return [load_by_quart(title, q.year, q.quarter) for q in Quarter(year, 1).iter_back(num)]
    elif month in [8, 9, 10]:
        return [load_by_quart(title, q.year, q.quarter) for q in Quarter(year, 2).iter_back(num)]
    elif month in [11, 12]:
        return [load_by_quart(title, q.year, q.quarter) for q in Quarter(year, 3).iter_back(num)]
    else:
        raise Exception(f"Invalid month: {month}")


def load_one(title: str, year: int, month: int) -> pandas.DataFrame:
    return load(title, year, month, 1)[0]

def validate():
    # fixme: 분기 재무데이터가 정상적인지를 당기 재무데이터로 검증
    pass