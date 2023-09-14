import logging
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from base import log
from utils.timeutil import YearQtr, month_to_quarter
from config import config
from core.repository.maria.conn import maria_home

log.init()

_logger = logging.getLogger(__name__)
_client = MongoClient(config["mongo"]["url"])
_col = _client["finance"]["ds_corp"]
_mariadb = maria_home()


def date_to_quarter(s: str):
    d = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").date()
    return YearQtr(year=d.year, qtr=month_to_quarter(d.month))


def doc_to_frame(doc: dict):
    """
    mongodb 다큐먼트를 DataFrame 으로 변경
    """
    data = doc["content"]["data"]["pods"][1]["content"]["data"]
    df = pd.DataFrame(data)
    df.columns = [col.split(" ")[0] for col in df.columns]
    yq = df["date"].apply(lambda x: date_to_quarter(x))
    df["year"] = yq.apply(lambda x: x.year)
    df["quarter"] = yq.apply(lambda x: x.qtr)
    df["code"] = df["symbol"].apply(lambda x: x.split(":")[1])
    df = df.drop(columns=["date", "symbol", "entity_name"])

    columns = ["code", "year", "quarter"]
    columns += [col for col in df.columns if col not in columns]
    return df[columns]


def main():
    """
    mongodb/finance/ds_corp 컬렉션의 모든 다큐먼트를 mariadb/finance/finance 테이블에 저장
    """
    count = 1
    for doc in _client["finance"]["ds_corp"].find({}):
        print(count)
        df = doc_to_frame(doc)
        df.to_sql("finance", _mariadb, if_exists="append", index=False)
        count += 1
