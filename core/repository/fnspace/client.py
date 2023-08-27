import numpy as np
import pandas as pd
import requests

from base.error import HttpRequestNotOk
from utils.pdutil import sort_columns
from config import config
from .error import FnSpaceRequestError
from typing import *


def _check_response(res):
    if res.status_code != 200:
        raise HttpRequestNotOk(res.status_code)

    body = res.json()
    if body["success"] != "true":
        raise FnSpaceRequestError(body)


def fetch_finance(
    code: str,
    item: str,
    sep: bool,
    year: int,
    month: int
) -> dict:
    year_month = f"{year}{str(month).zfill(2)}"
    code = f"A{code}" if len(code) == 6 else code
    return _call_api(
        code=code,
        item=item,
        consolgb="I" if sep else "C",
        annualgb="QQ",
        accdategb="C",
        fraccyear=year_month,
        toaccyear=year_month
    )


def _call_api(
    code: str,
    item: str,
    consolgb: str,
    annualgb: str,
    accdategb: str,
    fraccyear: str | int,
    toaccyear: str | int,
) -> dict:
    """
    :param code:		종목코드. 조회하고자 하는 종목 코드를 "," 로 연결. 예) 005930,005490
    :param item:		조회항목. 조회하고자 하는 항목을 ","로 연결. 각 API에서 조회 가능한 데이타 항목은 "API별 조회 데이터 목록" API를 통해 확인 가능
    :param consolgb:	default: 주재무제표(M). 회계기준. 주재무제표(M)/연결(C)/별도(I)
    :param annualgb:	default: 연간(A). 연간(A)/분기(QQ)/분기누적(QY)
    :param accdategb:	default: Calendar(C). 컨센서스 결산년월 선택 기준. Calendar(C)/Fiscal(F)
    :param fraccyear:   조회 시작 결산년월
    :param toaccyear:   조회 종료 결산년월
    """
    assert len(code) == 7 and code[0] == "A", "Invalid code"
    assert consolgb in ["M", "C", "I"]
    assert annualgb in ["A", "QQ", "QY"]
    assert accdategb in ["C", "F"]

    params = {
        "key": config["fnspace"]["key"],
        "code": code,
        "item": item,
        "consolgb": consolgb,
        "annualgb": annualgb,
        "accdategb": accdategb,
        "fraccyear": fraccyear,
        "toaccyear": toaccyear,
        "format": "json",
    }

    res = requests.get("https://www.fnspace.com/Api/FinanceApi", params=params)
    _check_response(res)
    body = res.json()
    return body


def _parse_response(body: dict) -> Optional[pd.DataFrame]:
    dataset = body["dataset"]
    if len(dataset) == 0:
        return None

    result = pd.DataFrame()
    for item in dataset:
        result = pd.concat([result, _parse_item_of_dataset(item, body["item"].split(","))])

    result["consolgb"] = body["consolgb"]
    result = result[sort_columns(result.columns, ["code", "NAME"], ["consolgb", "item", "value"])]
    return result


def _parse_item_of_dataset(item: dict, item_keys: list[str]) -> pd.DataFrame:
    df_all = pd.DataFrame(item["DATA"])
    columns = [c for c in df_all.columns if c not in item_keys]
    result = pd.DataFrame()
    for item_key in item_keys:
        sub_result = df_all[columns + [item_key]].rename(columns={item_key: "value"})
        sub_result["item"] = item_key
        sub_result["value"] *= 1000
        sub_result = sub_result.fillna(np.nan)
        result = pd.concat([result, sub_result])

    result["code"] = item["CODE"][-6:]
    result["NAME"] = item["NAME"]
    result = result.sort_values(by="DATE")
    result = result.reset_index(drop=True)
    return result
