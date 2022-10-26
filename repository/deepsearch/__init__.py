import json
import os

import pandas
from requests import get, Response
from resources import pre_queried_dir
from config import config


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
