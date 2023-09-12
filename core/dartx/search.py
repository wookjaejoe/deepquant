import re
from typing import *

import pandas as pd
import requests
from retry import retry

from core.dartx.apikey import OpenDartApiKey
from core.repository.maria.stocks import find_stock
from utils.timeutil import YearQuarter, YearMonth


@retry(tries=3, delay=1, jitter=10)
def search_reports(
    bgn_de: str = "19980101",
    end_de: str = None,
    stock_code: str = None,
    corp_code: str = None
) -> pd.DataFrame:
    """
    공시 검색
    """
    assert stock_code is not None or corp_code is not None
    if corp_code is None:
        corp_code = find_stock(stock_code)["corp_code"]

    page_no = 1
    df = pd.DataFrame()
    while True:
        res = requests.get(
            url="https://opendart.fss.or.kr/api/list.json",
            params={
                "crtfc_key": OpenDartApiKey.next(),
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "page_count": 100,
                "page_no": page_no,
                "pblntf_ty": "A",
                "last_reprt_at": "Y"
            }
        )
        body = res.json()
        if body["status"] == "013":
            break

        df = pd.concat([df, pd.DataFrame(body["list"])])

        if body["page_no"] >= body["total_page"]:
            break

        page_no += 1

    return df


def get_fnqtr(name: str, fnm: int) -> Optional[YearQuarter]:
    """
    리포트 이름으로부터 년도와 분기 정보를 획득한다. 현재는 12월 결산 기준보고서만 취급한다.
    """
    ym = get_fnym(name)
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


def get_fnym(name: str) -> Optional[YearMonth]:
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
