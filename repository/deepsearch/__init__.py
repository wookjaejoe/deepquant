import json
import os

import pandas
from requests import get, Response
from resources import pre_queried_dir
from config import config
from typing import *


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

    # fixme : series 고려
    return pandas.DataFrame(rows, index=data['symbol'])


def load_by_year(title: str, year: int):
    return _load(os.path.join(pre_queried_dir, title, f'{year}.json'), title)


def load_by_quart(title: str, year: int, quart: int):
    return _load(os.path.join(pre_queried_dir, title, f'{year}-{quart}.json'), title)


def load_all_by(title: str, year: int, to_quart: int, limit: int):
    assert to_quart in [1, 2, 3, 4]
    quart = to_quart
    for i in range(limit):
        yield load_by_quart(title, year, quart)
        if quart == 1:
            year -= 1
            quart = 4
        else:
            quart -= 1


# 1,2 - 작년3분기(11월 15일까지 발표), 작년2분기, 작년1분기, 제작년4분기
# 3,4 - 작년 당기데이터 조회
# 5,6,7 - 1분기(5월 15일까지 발표), 작년4분기, 작년3분기, 작년2분기 <- 이때도 당기 데이터 조회하면 코넥스 커버 가능
# 8,9,10 - 2분기(8월 15일까지 발표), 1분기, 작년4분기, 작년3분기
# 11,12 - 3분기(11월 15일까지 발표), 2분기, 1분기, 작년1분기
def load(title: str, year: int, month: int) -> List[pandas.DataFrame]:
    print("!!! 재무데이터 로드 !!!")
    if month in [1, 2]:
        print(title, year - 1, 3)
        print(title, year - 1, 2)
        print(title, year - 1, 1)
        print(title, year - 2, 4)
        return [
            load_by_quart(title, year - 1, 3),
            load_by_quart(title, year - 1, 2),
            load_by_quart(title, year - 1, 1),
            load_by_quart(title, year - 2, 4),
        ]
    elif month in [3, 4]:
        print(title, year - 1)
        return [load_by_year(title, year - 1)]
    elif month in [5, 6, 7]:
        # print(title, year - 1)  # 5, 6, 7의 경우 1분기 기업 실적 발표된 상황이지만, 전년 당기보고서가 더 수익율 좋은 것 같다.
        # return [load_by_year(title, year - 1)]
        print(title, year, 1)
        print(title, year - 1, 4)
        print(title, year - 1, 3)
        print(title, year - 1, 2)
        return [
            load_by_quart(title, year, 1),
            load_by_quart(title, year - 1, 4),
            load_by_quart(title, year - 1, 3),
            load_by_quart(title, year - 1, 2)
        ]
    elif month in [8, 9, 10]:
        print(title, year, 2)
        print(title, year, 1)
        print(title, year - 1, 4)
        print(title, year - 1, 3)
        return [
            load_by_quart(title, year, 2),
            load_by_quart(title, year, 1),
            load_by_quart(title, year - 1, 4),
            load_by_quart(title, year - 1, 3)
        ]
    elif month in [11, 12]:
        print(title, year, 3)
        print(title, year, 2)
        print(title, year, 1)
        print(title, year - 1, 4)
        return [
            load_by_quart(title, year, 3),
            load_by_quart(title, year, 2),
            load_by_quart(title, year, 1),
            load_by_quart(title, year - 1, 4)
        ]
    else:
        raise Exception(f"Invalid month: {month}")
