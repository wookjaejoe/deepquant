"""
OpenDart fnlttSinglAcntAll API 활용한 재무정보 수집
"""

import logging
from typing import *

import pandas as pd
import requests
from pymongo import MongoClient
from retry import retry

from config import config
from core.repository import get_stocks
from core.repository.dartx.apikey import OpenDartApiKey
from core.repository.dartx.search import get_fnqtr, get_fnym
from core.repository.dartx.search import search_reports

_logger = logging.getLogger(__name__)
_mongo_client = MongoClient(config["mongo"]["url"])
_mongo_clt = _mongo_client["finance"]["fnlttSinglAcntAll"]


def _opendart_url(path: str):
    return f"https://opendart.fss.or.kr/api/{path}"


@retry(tries=3, delay=1, jitter=10)
def _request_full_report(
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str
) -> dict:
    """
    crtfc_key	API 인증키	STRING(40)	Y	발급받은 인증키(40자리)
    corp_code	고유번호	STRING(8)	Y	공시대상회사의 고유번호(8자리)
                                        ※ 개발가이드 > 공시정보 > 고유번호 참고
    bsns_year	사업연도	STRING(4)	Y	사업연도(4자리) ※ 2015년 이후 부터 정보제공
    reprt_code	보고서 코드	STRING(5)	Y	1분기보고서 : 11013
                                            반기보고서 : 11012
                                            3분기보고서 : 11014
                                            사업보고서 : 11011
    fs_div	개별/연결구분	STRING(3)	Y	OFS:재무제표, CFS:연결재무제표
    """
    assert bsns_year >= 2015
    assert reprt_code in ["11013", "11012", "11014", "11011"]
    res = requests.get(
        url=_opendart_url("fnlttSinglAcntAll.json"),
        params={
            "crtfc_key": OpenDartApiKey.next(),
            "corp_code": corp_code,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
            "fs_div": fs_div
        }
    )
    return res.json()


reprt_codes = {
    1: "11013",
    2: "11012",
    3: "11014",
    4: "11011"
}


def guess_fnm(reports: pd.DataFrame) -> Optional[int]:
    """
    리포트 이름을 통해 결산월을 추정한다.
    """
    try:
        fnym = reports["report_nm"].apply(get_fnym)
        reports = reports.reindex(fnym.sort_values(ascending=False).index)
        bn_report = reports[reports["report_nm"].str.contains("사업보고서")].iloc[0]
        return get_fnym(bn_report["report_nm"]).month
    except:
        return


def _fetch_reports(
    stock: pd.Series,
    bgn_de: str = "20150101"
):
    """
    한 종목에 대해 보고서를 검색하고, fnlttSinglAcntAll 통해 분기별 재무제표를 조회하여 MongoDB에 저장한다.
    """

    # 보고서 조회
    reports = search_reports(bgn_de=bgn_de, corp_code=stock["corp_code"])
    if reports.empty:
        # 보고서 없으면 종료
        return

    fnm = guess_fnm(reports)
    if fnm is None:
        fnm = int(stock["acc_mt"])

    for _, report in reports.iterrows():
        fnym = get_fnym(report["report_nm"])
        fnqtr = get_fnqtr(report["report_nm"], fnm)

        if fnym is None or fnqtr is None:
            continue

        if fnym.year < 2015:
            # 2015년 이후 데이터만 취급
            continue

        for fs_div in ["CFS", "OFS"]:
            args = {
                "corp_code": report["corp_code"],
                "bsns_year": fnym.year,
                "reprt_code": reprt_codes[fnqtr.quarter],
                "fs_div": fs_div
            }

            if _mongo_clt.find_one({"args": args}) is not None:
                continue

            body = _request_full_report(**args)
            doc = {
                "report": report.to_dict(),
                "args": args,
                "body": body
            }
            _mongo_clt.insert_one(doc)


def fetch_reports():
    """
    모든 기업 재무제표 수집
    """
    stocks = get_stocks()

    num = 1
    for _, stock in stocks.iterrows():
        name = stock["stock_code"]
        _logger.info(f"[{num}/{len(stocks)}] {name}")
        _fetch_reports(stock)
        num += 1
