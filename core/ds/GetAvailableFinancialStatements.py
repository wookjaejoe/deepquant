import logging

import pandas as pd
import requests
from pymongo import MongoClient
from retry import retry

from config import config
from core.repository import get_stocks
from datetime import date
import base64

_client = MongoClient(config["mongo"]["url"])
_col = _client["ds"]["GetAvailableFinancialStatements"]

_logger = logging.getLogger()

auth = config["deepSearchAuth"]


@retry(tries=3, delay=1, jitter=3)
def call_api(code: str) -> dict:
    assert len(code) == 6
    _input = f"GetAvailableFinancialStatements(KRX:{code}, date_from=1990-01-01, date_to={date.today()})"
    _input = base64.b64encode(_input.encode("utf8")).decode("utf8")
    res = requests.get(
        "https://www.deepsearch.com/api/app/v1/compute",
        data="{\"input\":" + f"\"{_input}\"" + "}",
        headers={
            "authorization": "Basic 7c8z7pIPU9pBtHHphMdRoA==",
            "content-type": "application/json",
            "x-deepsearch-encoded-input": "true",
        }
    )
    assert res.status_code == 200, f"Status code: {res.status_code}"
    return res.json()


def collect_all():
    exists = [doc["code"] for doc in _col.aggregate([{"$project": {"code": 1}}])]
    stocks = get_stocks()
    codes = [code for code in stocks["stock_code"] if code not in exists]

    num = 0
    for code in codes:
        num += 1
        _logger.info(f"[{num}/{len(codes)}] {code}")
        # noinspection PyCallingNonCallable
        content = call_api(code)
        _col.insert_one({
            "code": code,
            "content": content
        })


def doc_to_frame(doc: dict) -> pd.DataFrame:
    return pd.DataFrame(doc["content"]["data"]["pods"][1]["content"]["data"])


def load(code: str):
    docs = list(_col.find({"code": code}))
    assert len(docs) == 1
    return doc_to_frame(docs[0])


def load_all(only_success: bool):
    flt = {"content.success": True} if only_success else {}
    result = pd.DataFrame()
    for doc in _col.find(flt):
        result = pd.concat([result, doc_to_frame(doc)])

    return result
