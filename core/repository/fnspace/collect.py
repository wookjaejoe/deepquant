import logging
from datetime import datetime

from pymongo import MongoClient

from config import config
from core.repository.fnspace.client import fetch_finance

_logger = logging.getLogger(__name__)
_mongo_client = MongoClient(config["mongo"]["url"])
_mongo_clt = _mongo_client["finance"]["fnSpaceFinanceApi"]


def _collect_if_not_exist(code: str, item: str, year: int, month: int, sep: bool):
    """
    MongoDB에 데이터 없으면 새로 API 호출해서 데이터 추가
    """
    body = fetch_finance(
        code=code,
        item=item,
        sep=sep,
        year=year,
        month=month
    )
    _mongo_clt.insert_one(body)


def collect_if_not_exist(code: str, year: int, month: int, item: str):
    params = {
        "code": code,
        "item": item,
        "year": year,
        "month": month,
        "sep": False
    }

    # 연결재무재표 없으면 수집
    if _mongo_clt.find_one({"params": params}) is None:
        body = fetch_finance(**params)
        _mongo_clt.insert_one({
            "params": params,
            "response": body,
            "requestTime": datetime.now()
        })

    # 별도재무데표 없으면 수집
    params["sep"] = True
    if _mongo_clt.find_one({"params": params}) is None:
        body = fetch_finance(**params)
        _mongo_clt.insert_one({
            "params": params,
            "response": body,
            "requestTime": datetime.now()
        })
