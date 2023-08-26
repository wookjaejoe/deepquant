import io
import zipfile

import pandas as pd
import requests
import os
from datetime import date

from core.repository.dartx.apikey import OpenDartApiKey
import tempfile
from core.repository import maria_home

import logging

_logger = logging.getLogger()


def _fetch_corps() -> pd.DataFrame:
    """
    opendart api 통해 종목 정보 가져오기
    """
    res = requests.get(
        "https://opendart.fss.or.kr/api/corpCode.xml",
        params={"crtfc_key": OpenDartApiKey.next()}
    )

    assert res.status_code == 200
    assert res.content

    zf = zipfile.ZipFile(io.BytesIO(res.content))
    xml_data = zf.read('CORPCODE.xml')

    # noinspection PyTypeChecker
    return pd.read_xml(
        xml_data,
        dtype={
            "corp_code": str,
            "stock_code": str
        }
    )


def company(corp_code: str):
    """
    status	에러 및 정보 코드	(※메시지 설명 참조)
    message	에러 및 정보 메시지	(※메시지 설명 참조)

    corp_name	    정식명칭	                        정식회사명칭
    corp_name_eng	영문명칭	                        영문정식회사명칭
    stock_name	    종목명(상장사) 또는 약식명칭(기타법인)	종목명(상장사) 또는 약식명칭(기타법인)
    stock_code	    상장회사인 경우 주식의 종목코드	        상장회사의 종목코드(6자리)
    ceo_nm	        대표자명	                        대표자명
    corp_cls	    법인구분	                        법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)
    jurir_no	    법인등록번호	                    법인등록번호
    bizr_no	        사업자등록번호	                    사업자등록번호
    adres	        주소	                            주소
    hm_url	        홈페이지	                        홈페이지
    ir_url	        IR홈페이지	                    IR홈페이지
    phn_no	        전화번호	                        전화번호
    fax_no	        팩스번호	                        팩스번호
    induty_code	    업종코드	                        업종코드
    est_dt	        설립일(YYYYMMDD)	                설립일(YYYYMMDD)
    acc_mt	        결산월(MM)	                    결산월(MM)
    """

    res = requests.get(
        "https://opendart.fss.or.kr/api/company.json",
        params={
            "crtfc_key": OpenDartApiKey.next(),
            "corp_code": corp_code
        }
    )
    body = res.json()
    assert body["status"] == "000", body["message"]
    return body


today = date.today().strftime("%Y%m%d")
corp_file = os.path.join(tempfile.gettempdir(), "deepquant", f"corp_{today}.pkl")
os.makedirs(os.path.dirname(corp_file), exist_ok=True)
stocks = pd.read_sql("stocks", maria_home())


def find_stock(stock_code: str):
    corp_list = stocks[stocks["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return stocks[stocks["stock_code"] == stock_code].iloc[0]


def update_stocks():
    """
    MariaDB > stocks 테이블 데이터 업데이트
    """
    _logger.info("Fetching corp list...")
    corps = _fetch_corps()
    corps = corps[corps["stock_code"].notna()]

    companies = []
    num = 1
    for corp_code in corps["corp_code"]:
        _logger.info(f"[{num}/{len(corps)}] Fetching company info...")
        companies.append(company(corp_code))
        num += 1

    corps = corps.merge(pd.DataFrame(companies), on="corp_code", how="left")
    table_name = "stocks_" + date.today().strftime("%Y%m%d")

    _logger.info(f"Updating {table_name}")
    corps.to_sql(table_name, maria_home(), index=False)
