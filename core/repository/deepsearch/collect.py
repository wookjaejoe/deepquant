"""
deepsearch 통해 재무데이터를 수집한다. 수집한 데이터는 mongodb에 저장한다.
"""


import logging
from datetime import date

import dart_fss as dart
from pymongo import MongoClient

from config import config
from core.repository.dartx import OpenDartApiKey
from core.repository.deepsearch.query import query2
from base import log
from core.repository.krx import get_ohlcv_latest
from retry import retry

log.init()

titles = [
    "자본총계", "자산총계", "유동자산",
    "매출액", "매출총이익", "영업이익", "당기순이익",
    "영업활동현금흐름"
]

_logger = logging.getLogger(__name__)
_client = MongoClient(config["mongo"]["url"])
_col = _client["finance"]["ds_corp"]

dart.set_api_key(OpenDartApiKey.next())


def collect(stock_code):
    _col.insert_one({
        "code": stock_code,
        "content": query2(stock_code, titles, 2000, 2023)
    })


@retry(tries=3, delay=10, logger=_logger)
def report_exists(corp):
    """
    opendart 에서 특정 종목 2000년부터 현재까지 리포트 존재하는지 여부 확인
    """
    dart.set_api_key(OpenDartApiKey.next())
    try:
        search_result = corp.search_filings(
            bgn_de="20000101",
            end_de=date.today().strftime('%Y%m%d'),
            last_reprt_at="Y",
            pblntf_ty="A",
        )
    except dart.errors.NoDataReceived:
        return False

    return len(search_result) > 0


def nho(s: str):
    """
    입력된 문자열이 n호로 끝나는지 여부 확인
    """
    try:
        int(s[-2])
        return s.endswith("호")
    except:
        return False


def main():
    """

    """

    # 스팩, n호 종목 제거
    corp_list = [c for c in dart.get_corp_list()
                 if c.stock_code and "스팩" not in c.corp_name and not nho(c.corp_name)]

    _logger.info(f"Ready for {len(corp_list)} corps")
    stocks = get_ohlcv_latest().set_index("code")

    def cap(stock_code):
        try:
            return stocks.loc[stock_code]["cap"]
        except:
            return -1

    # 수집할 종목 리스트를 시가총액 내림차순으로 정렬
    corp_list.sort(key=lambda c: cap(c.stock_code), reverse=True)

    for corp in corp_list:
        # 이미 수집된 종목 스킵
        if _col.count_documents({"code": corp.stock_code}) > 0:
            _logger.info(f"Skipping for {corp}")
            continue

        # opendart 에서 리포트 확인되지 않는 종목 스킵
        if not report_exists(corp):
            _logger.info(f"Skipping for {corp}")
            continue

        try:
            _logger.info(f"Collecting for {corp}")
            collect(corp.stock_code)
        except Exception as e:
            # _logger.error(f"Failure about {corp}", exc_info=e)
            _logger.error(f"Failure about {corp}")
