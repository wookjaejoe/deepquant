from __future__ import annotations

import json
import os
import zipfile
from dataclasses import dataclass
from enum import Enum
from typing import *

import pandas as pd
import xmltodict
from requests import get

from core.repository import maria
from core.repository.dartx import OpenDartApiKey


@dataclass
class DartCorp:
    corp_code: str
    corp_name: str
    stock_code: Optional[str]
    modify_date: str

    @staticmethod
    def from_dict(d: dict) -> DartCorp:
        return DartCorp(
            corp_code=d["corp_code"],
            corp_name=d["corp_name"],
            stock_code=d["stock_code"],
            modify_date=d["modify_date"]
        )


tempdir = ".temp"
os.makedirs(tempdir, exist_ok=True)


def resource_path(subpath: str):
    return os.path.join(tempdir, subpath)


def fetch_corps() -> Iterator[DartCorp]:
    response = get("https://opendart.fss.or.kr/api/corpCode.xml", params={"crtfc_key": OpenDartApiKey.next()})
    assert response.status_code == 200
    assert response.content

    zip_name = resource_path("corpCode.zip")
    with open(zip_name, 'wb') as f:
        f.write(response.content)

    unzip_dir = resource_path("corpCode")
    xml_file = resource_path("corpCode/corpCode.xml")
    with zipfile.ZipFile(zip_name, 'r') as f:
        f.extractall(unzip_dir)

    with open(xml_file) as f:
        raw = f.read()

    return [DartCorp.from_dict(element) for element in xmltodict.parse(raw)["result"]["list"]]


class ReportCode(Enum):
    Q1 = "11013"
    HY = "11012"
    Q3 = "11014"
    BUSINESS = "11011"


class DartResponseStatus(Enum):
    OK = 0
    NOK = 1

    @staticmethod
    def from_dict(content: dict):
        return DartResponseStatus.OK if content["status"] == "000" and content["message"] == "정상" \
            else DartResponseStatus.NOK

    @staticmethod
    def is_ok(content: dict):
        return DartResponseStatus.from_dict(content) == DartResponseStatus.OK


corp_codes: Dict[str, str] = {corp.stock_code: corp.corp_code for corp in fetch_corps()}


def fetch_all(year: int, report_code: ReportCode):
    for corp in maria.corp.get_corps()[700:]:
        filepath = resource_path(f"{year}-{report_code.name}/{corp.code}.json")
        if os.path.isfile(filepath):
            with open(filepath, 'r') as f:
                content = json.load(f)

            if DartResponseStatus.is_ok(content):
                continue

        url = f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        response = get(url, params={
            "crtfc_key": crtfc_key,
            "corp_code": corp_codes[corp.code],
            "bsns_year": year,
            "reprt_code": report_code.value,
            "fs_div": "CFS"
        })
        content = json.loads(response.content)
        # assert response.status_code == 200, f'Response code not 200, actual - {response.status_code}'
        # assert content
        # assert DartResponseStatus.is_ok(content)
        print(corp.code, corp.ver, content["status"], content["message"])
        with open(filepath, "w") as f:
            json.dump(content, f)

def corps():
    # 로컬에 활용가능한 캐시 있으면, 불러오기
    # 없으면, 새로 다운로드
    pass


def load(filepath: str):
    with open(filepath, 'r') as f:
        content = json.load(f)
        assert DartResponseStatus.is_ok(content)
        return pd.DataFrame(content["list"])


interest_colnames = {
    "account_id": "account_id",
    "thstrm_amount": "당기금액",
    "currency": "통화단위"
}
interest_index = {
    "ifrs-full_Equity": "자본총계",
    "ifrs-full_Assets": "자산총계",
    "ifrs-full_GrossProfit": "매출총이익",
    "ifrs-full_Revenue": "매출액",
    "ifrs-full_CostOfSales": "매출원가"
}


def load_all(year: int, report_code: ReportCode):
    result = pd.DataFrame()
    corps = list(maria.corp.get_corps())
    # corps = list([c for c in maria.corp.get_corps() if c.code == "005930"])
    total = len(corps)
    count = 0
    for corp in corps:
        count += 1
        print(f"{count}/{total}")
        filepath = resource_path(f"{year}-{report_code.name}/{corp.code}.json")
        try:
            df = load(filepath)
        except:
            print(f"{corp.code} - X")
            continue

        df = df[df["account_id"].isin(interest_index.keys())]
        df = df[df["sj_nm"] != "자본변동표"]
        df = df[interest_colnames.keys()]
        df.columns = list(interest_colnames.values())
        df.index = df["account_id"]
        df = df["당기금액"].to_frame(corp.code)
        result = result.merge(df, how='outer', left_index=True, right_index=True)

    result.T.to_csv("2022-Q3.csv")
    print(result)
    return result
