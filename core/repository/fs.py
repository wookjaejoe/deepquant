from core.repository import maria_home
import pandas as pd
from core.repository import get_stocks
from base.pdutil import sort_columns


def integrate():
    result = pd.read_sql("fnlttSinglAcntAll", maria_home())
    stocks = get_stocks()
    result = result.merge(stocks[["corp_code", "stock_code", "stock_name"]], on="corp_code", how="left")
    result = result.rename(columns={
        "bsns_year": "year",
        "stock_code": "code",
        "stock_name": "name"
    })

    result["fs_div"] = result["fs_div"].apply(lambda x: "연결" if x == "CFS" else "별도")
    result["qtr"] = result["reprt_code"].replace({
        "11013": 1,
        "11012": 2,
        "11014": 3,
        "11011": 4
    })
    result = result.drop(columns=["reprt_code"])
    return result[sort_columns(result.columns, forward=["code", "name"])]
