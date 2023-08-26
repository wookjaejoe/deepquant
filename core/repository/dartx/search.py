from core.repository.maria.stocks import find_stock
import requests
import pandas as pd
from retry import retry
from core.repository.dartx import OpenDartApiKey


@retry(tries=3, delay=1, jitter=10)
def search_reports(
    bgn_de: str = "19980101",
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
