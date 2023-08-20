import logging
import os
import tempfile
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
from pymongo import MongoClient
from retry import retry

from base import log
from config import config
from core.repository.dartx import OpenDartApiKey, OpenDartRequest

log.init()
_logger = logging.getLogger()
_client = MongoClient(config["mongo"]["url"])
_collection = _client["finance"]["opendart"]

_collection.create_index([
    ("request.corp_code", 1),
    ("request.bsns_year", 1),
    ("request.reprt_code", 1),
    ("request.fs_div", 1)
], name="default_compound_index", unique=True)

_collection.create_index([
    ("response.body.status", 1),
], name="response_status")


def find(
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str
):
    return _collection.find({
        "request.corp_code": corp_code,
        "request.bsns_year": bsns_year,
        "request.reprt_code": reprt_code,
        "request.fs_div": fs_div,
    })


def load_corps():
    _logger.info("Fetching corp codes from opendart...")
    res = OpenDartRequest.get("corpCode.xml", crtfc_key=OpenDartApiKey.next())

    with tempfile.TemporaryDirectory() as tempdir:
        tempzip = os.path.join(tempdir, "temp.zip")

        _logger.info(f"Writting {tempzip} ...")
        with open(tempzip, "wb") as f:
            f.write(res.content)

        _logger.info(f"Extracting {tempzip} ...")
        with ZipFile(tempzip) as z:
            z.extractall(tempdir)

        xmlfile = os.path.join(tempdir, "CORPCODE.xml")
        _logger.info(f"Loading {xmlfile}")
        with open(xmlfile) as f:
            content = f.read()
            df = pd.read_xml(content, dtype={"corp_code": str, "stock_code": str})

        return df[~df["stock_code"].isna()]


@retry(tries=3, delay=60, logger=_logger)
def collect_report(
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str
):
    """
        corp_code	고유번호        STRING(8)	Y	공시대상회사의 고유번호(8자리)
        bsns_year	사업연도        STRING(4)	Y	사업연도(4자리) ※ 2015년 이후 부터 정보제공
        reprt_code	보고서 코드      STRING(5)	Y   1분기보고서 : 11013, 반기보고서 : 11012, 3분기보고서 : 11014, 사업보고서 : 11011
        fs_div      개별/연결구분     STRING(3)	Y	CFS:연결재무제표, OFS:재무제표
        """

    params = {
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div
    }
    res = OpenDartRequest.get(
        "fnlttSinglAcntAll.json",
        **params
    )
    body = res.json()

    if body["status"] not in ["000", "013"]:
        _logger.error("상태이상")
        _logger.error(f"body: {body}")
        exit()

    doc = {
        "request": {
            **params
        },
        "response": {
            "body": body
        },
        "created": datetime.now()
    }

    _collection.insert_one(doc)


def search_reports(corp_code: str):
    """
    pblntf_detail_ty https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001 상세유형 참고
    """

    result = pd.DataFrame()
    for page_no in range(1, 10):
        res = OpenDartRequest.get(
            "list.json",
            corp_code=corp_code,
            bgn_de="19900101",
            page_count=100,
            page_no=page_no,
            pblntf_ty="A"
        )
        body = res.json()

        if body["status"] == "013":
            break

        df = pd.DataFrame(body["list"])
        result = pd.concat([result, df]).reset_index(drop=True)

        if body["page_no"] >= body["total_page"]:
            break

    return result


# noinspection PyCallingNonCallable
def collect(corp_code: str):
    result = search_reports(corp_code)
    if len(result) == 0:
        return

    first_date = result["rcept_dt"].min()
    first_year = int(first_date[:4])
    last_date = result["rcept_dt"].max()
    last_year = int(last_date[:4])
    print(corp_code, first_year, last_year)
    # 첫해부터 시작해서, 분기별 데이터 수집...
    for year in range(first_year - 1, last_year + 1):
        for reprt_code in ["A11013", "A11012", "A11014", "A11011"]:
            params = {
                "corp_code": corp_code,
                "bsns_year": year,
                "reprt_code": reprt_code,
                "fs_div": "OFS"
            }

            # 개별 재무제표
            if len(list(find(**params))) == 0:
                collect_report(**params)

            # 연결 재무제표
            params["fs_div"] = "CFS"
            if len(list(find(**params))) == 0:
                collect_report(**params)


def main():
    OpenDartApiKey.next()
    OpenDartApiKey.remove_invalid_keys()

    # 모든 취급종목 로드
    corps = load_corps()
    total = len(corps)
    i = 0

    for corp_code in corps["corp_code"]:
        i += 1
        print(f"[{i}/{total}] {corp_code}")
        collect(corp_code)
