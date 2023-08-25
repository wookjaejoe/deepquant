import io
import zipfile

import pandas as pd
import requests
import os
from datetime import date

from core.repository.dartx.apikey import OpenDartApiKey
import tempfile


def _fetch() -> pd.DataFrame:
    """
    opendart api 통해 종목 정보 가져오기
    """
    response = requests.get(
        "https://opendart.fss.or.kr/api/corpCode.xml",
        params={"crtfc_key": OpenDartApiKey.next()}
    )

    assert response.status_code == 200
    assert response.content

    zf = zipfile.ZipFile(io.BytesIO(response.content))
    xml_data = zf.read('CORPCODE.xml')

    # noinspection PyTypeChecker
    return pd.read_xml(
        xml_data,
        dtype={
            "corp_code": str,
            "stock_code": str
        }
    )


today = date.today().strftime("%Y%m%d")
corp_file = os.path.join(tempfile.gettempdir(), "deepquant", f"corp_{today}.pkl")
os.makedirs(os.path.dirname(corp_file), exist_ok=True)


def _load():
    if os.path.isfile(corp_file):
        return pd.read_pickle(corp_file)
    else:
        df = _fetch()
        df.to_pickle(corp_file)
        return df


corps = _load()
stocks = corps[corps["stock_code"].notna()]


def find_stock(stock_code: str):
    corp_list = stocks[stocks["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return stocks[stocks["stock_code"] == stock_code].iloc[0]
