"""
여러 출처로부터 수집한 재무데이터로 재무재표 테이블을 구성하고 관리한다.

Layer1
- MongoDB/fnlttSinglAcntAll

Layer2
- MariaDB/fs
"""

import logging

import numpy as np
import pandas as pd
from pymongo import MongoClient

from config import config
from core.repository import maria_home
from core.repository import get_stocks, find_corp
from core.dartx.search import get_fnym
from utils import pdutil

_logger = logging.getLogger(__name__)
_mongo_client = MongoClient(config["mongo"]["url"])
_mongo_clt = _mongo_client["finance"]["fnlttSinglAcntAll"]

_accounts = {
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

stocks = get_stocks()
qtr_by_reprt_code = {
    "11013": 1,
    "11012": 2,
    "11014": 3,
    "11011": 4
}

fs_div_kr = {
    "CFS": "연결",
    "OFS": "별도"
}


def _preprocess(doc: dict):
    """
    Opendart > fnlttSinglAcntAll API 요청과 응답정보를 담은 MongoDB fnlttSinglAcntAll 컬렉션의 다큐먼트 하나에 대한 전처리를 수행한다.
    응답 원본에서 sj_div와 account_id를 통해 주요 계정 항목을 찾는다. 응답 원본에는 account_id 중복 또는 누락이 있을 수 있다.
    """
    report = doc["report"]
    args = doc["args"]
    body = doc["body"]

    stock = find_corp(report["corp_code"])
    fnym = get_fnym(report["report_nm"])
    raw = pd.DataFrame(body["list"])
    result = pd.Series({
        "code": stock["stock_code"],
        "name": stock["stock_name"],
        "year": fnym.year,
        "month": fnym.month,
        "qtr": qtr_by_reprt_code[args["reprt_code"]],
        "fs_div": fs_div_kr[args["fs_div"]]
    })

    for acc_name, acc_ids in _accounts.items():
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

        if value == 0:
            value = np.nan

        result[acc_name] = value

    return result


def preprocess_all():
    """
    Opendart > fnlttSinglAcntAll API 요청과 응답정보를 담은 MongoDB fnlttSinglAcntAll 컬렉션 전체에 대하여 전처리를 수행하고,
    MariaDB fs 테이블에 저장한다.
    """
    maria_db = maria_home()

    # 이미 존재하는
    pk = ["code", "year", "month", "qtr", "fs_div"]
    pk_str = ", ".join(pk)
    exist_rows = pd.read_sql_query(
        f"select {pk_str} from fs",
        maria_db
    )
    num = 0
    buffer = pd.DataFrame()
    buffer_size = 10
    for doc in _mongo_clt.find({"body.status": "000"}):
        num += 1
        print(num)

        row = _preprocess(doc).to_frame().T
        if not pdutil.find(exist_rows, **row[pk].iloc[0].to_dict()).empty:
            continue

        buffer = pd.concat([buffer, row])
        if len(buffer) >= buffer_size:
            # 데이터 업로드
            buffer.to_sql("fs", maria_db, if_exists="append", index=False)
            buffer = pd.DataFrame()

    # 남은 데이터 업로드
    if not buffer.empty:
        buffer.to_sql("fs", maria_db, if_exists="append", index=False)
