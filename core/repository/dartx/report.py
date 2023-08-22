import requests

from core.repository.dartx.apikey import OpenDartApiKey
from core.repository.dartx.corps import corps, find_corp
from core.repository.dartx.error import NoData
import pandas as pd
from datetime import date
from itertools import product
from base.log import logger
from retry import retry


def opendart_url(path: str):
    return f"https://opendart.fss.or.kr/api/{path}"


def search_reports(
    bgn_de: str = "19980101",
    stock_code: str = None,
    corp_code: str = None
):
    assert stock_code is not None or corp_code is not None
    if corp_code is None:
        corp_code = find_corp(stock_code)["corp_code"]

    page_no = 1
    df = pd.DataFrame()
    while True:
        res = requests.get(
            url=opendart_url("list.json"),
            params={
                "crtfc_key": OpenDartApiKey.next(),
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "page_count": 100,
                "page_no": page_no,
                "pblntf_ty": "A"
            }
        )
        body = res.json()
        if body["status"] == "013":
            raise NoData()

        df = pd.concat([df, pd.DataFrame(body["list"])])

        if body["page_no"] >= body["total_page"]:
            break

        page_no += 1

    return df


def read_report(
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str
) -> pd.DataFrame:
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
        url=opendart_url("fnlttSinglAcntAll.json"),
        params={
            "crtfc_key": OpenDartApiKey.next(),
            "corp_code": corp_code,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
            "fs_div": fs_div
        }
    )
    json = res.json()
    if json["status"] == "013":
        raise NoData()

    df = pd.DataFrame(json["list"])
    df["bsns_year"] = df["bsns_year"].astype(int)
    return df


@retry(tries=3, delay=1, jitter=5)
def read_all_reports(corp_code: str) -> pd.DataFrame:
    # 2015-현재, 보고서종류, 연결구분에 대한 모든 조합
    args = pd.DataFrame(
        product(
            list(range(2015, date.today().year + 1)),
            ["11013", "11012", "11014", "11011"],
            ["OFS", "CFS"]
        ),
        columns=["bsns_year", "reprt_code", "fs_div"]
    )

    result = pd.DataFrame()
    for index, arg in args.iterrows():
        try:
            df = read_report(
                corp_code=corp_code,
                bsns_year=arg["bsns_year"],
                reprt_code=arg["reprt_code"],
                fs_div=arg["fs_div"],
            )
            df["fs_div"] = arg["fs_div"]
            result = pd.concat([result, df], ignore_index=True)
        except NoData:
            logger.info(f"No data for {arg.values}")
            pass

    return result
