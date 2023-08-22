import io
import zipfile

import pandas as pd
import requests

from core.repository.dartx.apikey import OpenDartApiKey


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


corps = _fetch()
stocks = corps[corps["stock_code"].notna()]


def find_corp(stock_code: str):
    corp_list = corps[corps["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return corps[corps["stock_code"] == stock_code].iloc[0]
