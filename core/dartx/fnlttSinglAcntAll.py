"""
OpenDart fnlttSinglAcntAll API 활용한 재무정보 수집
"""
import pandas as pd
import requests
from pandas import DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_not_exception_type

from core.dartx.apikey import OpenDartApiKey


def _opendart_url(path: str):
    return f"https://opendart.fss.or.kr/api/{path}"


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_not_exception_type(AssertionError)
)
def request_report(
    corp_code: str,
    bsns_year: int,
    reprt_code: str,
    fs_div: str
) -> DataFrame:
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
        url="https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
        params={
            "crtfc_key": OpenDartApiKey.next(),
            "corp_code": corp_code,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
            "fs_div": fs_div
        }
    )
    assert res.status_code == 200, f"Status code is {res.status_code}"
    res_json = res.json()
    assert res_json["status"] == "000", res_json["message"]
    return pd.DataFrame(res_json["list"])
