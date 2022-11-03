import json
import os
from dataclasses import dataclass

import pandas
from .conn import MariaConnection
from resources import pre_queried_dir
from typing import *


@dataclass
class Corp:
    code: str
    name: str


def upload_all_corp_from_pre_queried():
    """
    미리 쿼리된(pre_queried) 데이터에 포함된 (symbol, entity_name) 정보를 토대로, 존재하는 모든 (종목코드, 종목명) 정보를 DB에 upsert 함.
    """
    name_by_code = {}
    for root, subdirs, files in os.walk(pre_queried_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            print(f'Loading {file_path}')
            with open(file_path) as f:
                d = json.load(f)
            content = d['data']['pods'][1]['content']
            data = content['data']
            rows = data['entity_name']
            df = pandas.DataFrame(
                rows,
                index=data['symbol']
            )

            for idx, row in df.iterrows():
                idx: str
                spl = idx.split(':')
                assert spl[0] == 'KRX'
                code = spl[1]
                name = row[0]
                name_by_code[code] = name

    print(f'Upserting {len(name_by_code)} records...')
    with MariaConnection() as connection:
        cursor = connection.cursor()
        for stock in [Corp(code, name) for code, name in name_by_code.items()]:
            query = f"""
            INSERT INTO corp(code, name) VALUES('{stock.code}', '{stock.name}') 
            ON DUPLICATE KEY UPDATE code='{stock.code}', name='{stock.name}';
            """.strip()
            affected_rows = cursor.execute(query)
            print(f'{affected_rows} records updated.')

        connection.commit()


def fetch_all() -> Iterator[Corp]:
    with MariaConnection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * from corp")

    return [Corp(code=record[0], name=record[1]) for record in cursor.fetchall()]


_cache: Dict[str, Corp] = {corp.code: corp for corp in fetch_all()}


def get_holdings_corp() -> Iterator[Corp]:
    for code, corp in _cache.items():
        name = corp.name
        if "지주" in name or "홀딩스" in name:
            yield corp


def get_name(code: str) -> str:
    return _cache[code].name


def exists(code: str) -> bool:
    return code in _cache.keys()
