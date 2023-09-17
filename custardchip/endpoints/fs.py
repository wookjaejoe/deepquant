import pandas as pd

from core.fs import load_ifrs
from core.fs.alpha import FsAlpha
from core.repository import maria_home
from core.repository.maria.stocks import get_stocks
from custardchip.app import app
from custardchip.limit import limit_by_address

from starlette.requests import Request

stocks = get_stocks()
fs_alpha = FsAlpha()
fs_db = maria_home("fs")
ifrs = load_ifrs()
ifrs = ifrs[ifrs["LV"] <= 2]
type_ids = ["F", "B", "T", "K"]
limit_opt = "100/minute"


def type_to_qtr(type_id: str):
    return type_ids.index(type_id)


def qtr_to_type(qtr: int):
    return type_ids[qtr - 1]


@app.get("/stocks")
@limit_by_address(limit_opt)
def get_stocks(request: Request):
    result = pd.DataFrame({"code": fs_alpha.codes})
    result = result.merge(stocks, left_on="code", right_on="stock_code")
    result["exchange"] = result["corp_cls"].replace({
        "Y": "KOSPI",
        "K": "KOSDAQ",
        "N": "KONEX"
    })
    result = result.rename(columns={
        "stock_name": "name",
    })
    result = result[["code", "name", "exchange"]]
    return result.reset_index(drop=True).to_dict("records")


@app.get("/reports/{code}")
@limit_by_address(limit_opt)
def fs(request: Request, code: str):
    result = fs_alpha.reports(code)
    result["qtr"] = result["type_id"].replace({qtr_to_type(qtr): qtr for qtr in [1, 2, 3, 4]})
    result = result[["date", "qtr", "consolidated"]]
    return result.reset_index(drop=True).to_dict("records")


@app.get("/fs/{code}")
@limit_by_address(limit_opt)
def fs_detail(
    request: Request,
    code: str,
    year: int,
    qtr: int,
    consolidated: int
):
    assert qtr in [1, 2, 3, 4]
    type_id = qtr_to_type(qtr)
    result = pd.read_sql(f"""
    select * from `{code}`
    where year(date) = {year} and type_id = '{type_id}' and consolidated = {consolidated} 
    """, fs_db)
    result = result.merge(ifrs, on=["consolidated", "report_id", "account_id"])
    result["name"] = result.apply(lambda row: row["account_name"].split(row["재무제표"] + "/")[1], axis=1)
    result = result[["date", "재무제표", "name", "consolidated", "value"]].rename(columns={
        "재무제표": "sheet",
    })
    result = result[result["sheet"].isin(["재무상태표", "포괄손익계산서", "현금흐름표"])]
    return result.reset_index(drop=True).to_dict("records")
