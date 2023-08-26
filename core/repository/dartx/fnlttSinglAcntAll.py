"""
OpenDart fnlttSinglAcntAll API 활용한 재무정보 수집
"""

import logging
import re
from typing import *

import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient
from retry import retry

from base import pdutil
from base.timeutil import YearQuarter
from config import config
from core.repository import maria_home
from core.repository.dartx.apikey import OpenDartApiKey
from core.repository import get_stocks
from core.repository.dartx.search import search_reports
from base.timeutil import YearMonth

_logger = logging.getLogger(__name__)
_mongo_client = MongoClient(config["mongo"]["url"])
_mongo_clt = _mongo_client["finance"]["fnlttSinglAcntAll"]


def _opendart_url(path: str):
    return f"https://opendart.fss.or.kr/api/{path}"


def _fnqtr(name: str, fnm: int) -> Optional[YearQuarter]:
    """
    리포트 이름으로부터 년도와 분기 정보를 획득한다. 현재는 12월 결산 기준보고서만 취급한다.
    """
    ym = _fnym(name)
    if ym is None:
        return

    y, m = ym.year, ym.month
    result = None
    if "분기보고서" in name:
        if m == (fnm + 3) % 12:
            # 1분기: 결산월+3개월
            result = YearQuarter(y, 1)
        elif m == (fnm + 9) % 12:
            # 3분기: 결산월+9개월
            result = YearQuarter(y, 3)
    elif "반기보고서" in name and m == (fnm + 6) % 12:
        result = YearQuarter(y, 2)
    elif "사업보고서" in name and m == fnm:
        result = YearQuarter(y, 4)

    return result


def _fnym(name: str) -> Optional[YearMonth]:
    """
    리포트 이름으로부터 년도와 분기 정보를 획득한다. 현재는 12월 결산 기준보고서만 취급한다.
    """
    pattern = r'\d{4}.\d{2}'
    match = re.search(pattern, name)
    if match is None:
        return

    year_month = match.group()
    y, m = year_month.split(".")
    return YearMonth(int(y), int(m))


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
        fnym = reports["report_nm"].apply(_fnym)
        reports = reports.reindex(fnym.sort_values(ascending=False).index)
        bn_report = reports[reports["report_nm"].str.contains("사업보고서")].iloc[0]
        return _fnym(bn_report["report_nm"]).month
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
        fnm = stock["acc_mt"]

    for _, report in reports.iterrows():
        fnym = _fnym(report["report_nm"])
        fnqtr = _fnqtr(report["report_nm"], fnm)

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
    stocks = stocks[stocks["corp_cls"] != 'E']
    stocks = stocks[~stocks["stock_name"].str.endswith("스팩")]
    stocks = stocks[stocks["stock_code"].str.endswith("0")]

    num = 1
    for _, stock in stocks.iterrows():
        name = stock["stock_code"]
        _logger.info(f"[{num}/{len(stocks)}] {name}")
        _fetch_reports(stock)
        num += 1


accounts = {
    "자산총계": [
        "BS/ifrs-full_Assets",
        "BS/ifrs_Assets"
    ],
    "자본총계": [
        "BS/ifrs-full_Equity",
        "BS/ifrs_Equity"
    ],
    "유동자산": [
        "BS/ifrs-full_CurrentAssets",
        "BS/ifrs_CurrentAssets"
    ],
    "매출액": [
        "IS/ifrs-full_Revenue",
        "IS/ifrs_Revenue",
        "CIS/ifrs-full_Revenue",
        "CIS/ifrs_Revenue",
    ],
    "매출총이익": [
        "IS/ifrs-full_GrossProfit",
        "IS/ifrs_GrossProfit",
        "CIS/ifrs-full_GrossProfit",
        "CIS/ifrs_GrossProfit",
    ],
    "영업이익": [
        "IS/dart_OperatingIncomeLoss",
        "CIS/dart_OperatingIncomeLoss",
    ],
    "당기순이익": [
        "IS/ifrs-full_ProfitLoss",
        "IS/ifrs_ProfitLoss",
        "CIS/ifrs-full_ProfitLoss",
        "CIS/ifrs_ProfitLoss",
    ],
    "영업활동현금흐름": [
        "CF/ifrs-full_CashFlowsFromUsedInOperatingActivities",
        "CF/ifrs_CashFlowsFromUsedInOperatingActivities"
    ]
}


def _preprocess(doc: dict):
    """
    Opendart > fnlttSinglAcntAll API 요청과 응답정보를 담은 MongoDB fnlttSinglAcntAll 컬렉션의 다큐먼트 하나에 대한 전처리를 수행한다.
    응답 원본에서 sj_div와 account_id를 통해 주요 계정 항목을 찾는다. 응답 원본에는 account_id 중복 또는 누락이 있을 수 있다.
    """
    args = doc["args"]
    body = doc["body"]
    raw = pd.DataFrame(body["list"])
    result = pd.Series(args)
    for acc_name, acc_ids in accounts.items():
        # df에서 account_id가 acc_id에 포함되는 항목 찾기
        raw["sj_div/account_id"] = raw["sj_div"] + "/" + raw["account_id"]
        rows = raw[raw["sj_div/account_id"].isin(acc_ids)]
        values = rows["thstrm_amount"].replace("", np.nan).dropna().astype(int)
        if len(values) == 0:
            # 없을때, 버림
            continue
        elif len(values) == 1:
            # 하나 있을때, 취함
            value = values.iloc[0]
        else:
            # 첫번째 값을 취함. 본 코드에서 데이터 오염 발생 우려 있음.
            value = values.iloc[0]

        result[acc_name] = value

    return result


def preprocess_all():
    """
    Opendart > fnlttSinglAcntAll API 요청과 응답정보를 담은 MongoDB fnlttSinglAcntAll 컬렉션 전체에 대하여 전처리를 수행하고,
    MariaDB fnlttSinglAcntAll 테이블에 저장한다.
    """

    maria_db = maria_home()

    # 이미 존재하는
    exist_rows = pd.read_sql_query(
        "select corp_code, bsns_year, reprt_code, fs_div from fnlttSinglAcntAll",
        maria_db
    )
    num = 0
    buffer = pd.DataFrame()
    buffer_size = 500
    for doc in _mongo_clt.find({"body.status": "000"}):
        num += 1
        print(num)

        if not pdutil.find(exist_rows, **doc["args"]).empty:
            continue

        row = _preprocess(doc).to_frame().T
        buffer = pd.concat([buffer, row])
        if len(buffer) >= buffer_size:
            # 데이터 업로드
            buffer.to_sql("fnlttSinglAcntAll", maria_db, if_exists="append", index=False)
            buffer = pd.DataFrame()

    # 남은 데이터 업로드
    if not buffer.empty:
        buffer.to_sql("fnlttSinglAcntAll", maria_db, if_exists="append", index=False)
