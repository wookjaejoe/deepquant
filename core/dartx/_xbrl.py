"""
DEPRECATED 사용 안함
dart_fss 라이브러리를 이용한 재무데이터 수집
"""

import dart_fss
from dart_fss.filings import search
from dart_fss.fs.extract import analyze_xbrl
from pymongo import MongoClient

from utils import pdutil
from config import config
from core.dartx import OpenDartApiKey
from datetime import datetime

import logging

_logger = logging.getLogger()
_mongo_client = MongoClient(config["mongo"]["url"])
_mongo_clt = _mongo_client["finance"]["fnlttXbrl"]


def collect(
    corp_code: str,
    bgn_de="20120101"
):
    """
    항 종목 전체 기간 재무데이터 조회 및 수집
    """
    _logger.info(f"Searching for {corp_code}")
    dart_fss.set_api_key(OpenDartApiKey.next())
    reports = search(
        corp_code=corp_code,
        bgn_de=bgn_de,
        pblntf_ty="A",
        last_reprt_at="Y",
        page_count=100,
    )

    nos = [d["rcept_no"] for d in find(corp_code)]
    reports = [r for r in reports if r.rcept_no not in nos]
    _logger.info(f"{len(reports)} new reports.")
    for r in reports:
        _logger.info(f"Analyzing {r.corp_name} > {r.report_nm}")
        dart_fss.set_api_key(OpenDartApiKey.next())
        fs = analyze_xbrl(r)
        fs_sep = analyze_xbrl(r, separate=True)
        doc = {
            "corp_name": r.corp_name,
            "corp_code": r.corp_code,
            "stock_code": r.stock_code,
            "report_nm": r.report_nm,
            "rcept_no": r.rcept_no,
            "flr_nm": r.flr_nm,
            "rcept_dt": r.rcept_dt,
            "rm": r.rm,
            "request_time": datetime.now(),
            "fs": {k: pdutil.serialize(df) if df is not None else None for k, df in fs.items()},
            "fs_sep": {k: pdutil.serialize(df) if df is not None else None for k, df in fs_sep.items()}
        }
        _mongo_clt.insert_one(doc)


def find(corp_code: str):
    for doc in _mongo_clt.find({"corp_code": corp_code}):
        doc["fs"] = {
            k: pdutil.deserialize(b64) if b64 is not None else None
            for k, b64 in doc["fs"].items()
        }
        doc["fs_sep"] = {
            k: pdutil.deserialize(b64) if b64 is not None else None
            for k, b64 in doc["fs_sep"].items()
        }
        yield doc
