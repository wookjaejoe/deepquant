"""
1. make_table 함수 실행 - fs_yyyy_qq 테이블 생성
2. apply_to_fs 함수 실행 - fs_yyyy_qq fs 테이블에 수집한 내용 반영
"""

import calendar
from datetime import date
from datetime import timedelta

import pandas as pd

from core.dartx.fnlttSinglAcntAll import request_report
from core.dartx.search import search_reports
from core.repository import maria_home
from utils import pdutil

db = maria_home()


def list_reports(year: int, qtr: int):
    """
    리포트 목록 생성. 결산년월이 12월인 리포트만 취급
    """
    # A001: 사업보고서, A002: 반기보고서, A003: 분기보고서
    if qtr in [1, 3]:
        pblntf_detail_ty = "A003"
    elif qtr == 2:
        pblntf_detail_ty = "A002"
    elif qtr == 4:
        pblntf_detail_ty = "A001"
    else:
        raise ValueError("qtr must be in [1, 2, 3, 4]")

    fs_month = qtr * 3
    bgn_de = date(year, fs_month, calendar.monthrange(year, fs_month)[1])
    df = search_reports(
        bgn_de=bgn_de.strftime("%Y%m%d"),
        end_de=(bgn_de + timedelta(days=90)).strftime("%Y%m%d"),
        pblntf_detail_ty=pblntf_detail_ty
    )
    df = df[df["stock_code"].apply(lambda x: len(x.strip()) == 6)]
    df = df[df["report_nm"].str.contains(f"{year}.{fs_month:02}")]
    return df.set_index("stock_code")


def make_table(year: int, qtr: int):
    reports = list_reports(year, qtr)
    for stock_code, row in reports.iterrows():
        corp_code = row["corp_code"]
        year = 2023
        report_code = "11014"

        print(stock_code, row["flr_nm"])
        for fs_div in ["CFS", "OFS"]:
            try:
                df = request_report(
                    corp_code=corp_code,
                    bsns_year=year,
                    reprt_code=report_code,
                    fs_div=fs_div
                )
            except AssertionError as e:
                print(e)
                continue

            df.insert(0, "stock_code", stock_code)
            df.insert(0, "fs_div", fs_div)
            df.to_sql(f"fs_{year}_{qtr}Q", db, if_exists="append", index=False)


accounts = {
    "BS/ifrs_Assets": "자산총계",
    "BS/ifrs_Equity": "자본총계",
    "BS/ifrs_CurrentAssets": "유동자산",
    "BS/ifrs_CurrentLiabilities": "유동부채",
    "IS/ifrs_Revenue": "매출",
    "IS/ifrs_GrossProfit": "매출총이익",
    "IS/dart_OperatingIncomeLoss": "영업이익",
    "IS/ifrs_ProfitLossBeforeTax": "법인세비용차감전계속영업이익",
    "IS/ifrs_ProfitLoss": "당기순이익",
    "CIS/ifrs_Revenue": "매출",
    "CIS/ifrs_GrossProfit": "매출총이익",
    "CIS/dart_OperatingIncomeLoss": "영업이익",
    "CIS/ifrs_ProfitLossBeforeTax": "법인세비용차감전계속영업이익",
    "CIS/ifrs_ProfitLoss": "당기순이익",
    "CF/ifrs_CashFlowsFromUsedInOperatingActivities": "영업활동현금흐름",

    "BS/ifrs-full_Assets": "자산총계",
    "BS/ifrs-full_Equity": "자본총계",
    "BS/ifrs-full_CurrentAssets": "유동자산",
    "BS/ifrs-full_CurrentLiabilities": "유동부채",
    "IS/ifrs-full_Revenue": "매출",
    "IS/ifrs-full_GrossProfit": "매출총이익",
    "IS/ifrs-full_ProfitLossBeforeTax": "법인세비용차감전계속영업이익",
    "IS/ifrs-full_ProfitLoss": "당기순이익",
    "CIS/ifrs-full_Revenue": "매출",
    "CIS/ifrs-full_GrossProfit": "매출총이익",
    "CIS/ifrs-full_ProfitLossBeforeTax": "법인세비용차감전계속영업이익",
    "CIS/ifrs-full_ProfitLoss": "당기순이익",
    "CF/ifrs-full_CashFlowsFromUsedInOperatingActivities": "영업활동현금흐름",
}


def apply_to_fs(
    fs_year: int,
    fs_month: int,
    fs_day: int,
    fs_qtr: int
):
    """
    수집한 보고서 내용을 fs 테이블에 반영

    fs 테이블에 동일 년월 데이터가 이미 존재하면 skip
    """
    columns = "fs_div, stock_code, sj_div, account_id, thstrm_amount"
    account_ids = ", ".join([f"'{key}'" for key in accounts.keys()])
    table_name = f"fs_{fs_year}_{fs_qtr}Q"
    query = f"select {columns} from {table_name} where concat(sj_div, \"/\", account_id) in ({account_ids})"
    df = pd.read_sql(query, db)

    df["account"] = (df['sj_div'] + '/' + df['account_id']).replace(accounts)
    df["thstrm_amount"] = pd.to_numeric(df["thstrm_amount"], errors="coerce").astype('Int64')
    df = df.groupby(["stock_code", "fs_div"]).apply(
        lambda grp: grp.pivot_table(columns="account", values="thstrm_amount", aggfunc=lambda x: x.iloc[0])
    )

    df = df.droplevel(2).reset_index()
    df = df.rename(columns={"stock_code": "code", "fs_div": "consolidated"})
    df["date"] = date(fs_year, fs_month, fs_day)
    df["year"] = fs_year
    df["month"] = fs_month
    df["qtr"] = fs_qtr
    df["consolidated"] = df["consolidated"].replace({"CFS": 1, "OFS": 0})
    df = df[pdutil.sort_columns(df.columns, forward=["code", "date", "year", "month", "qtr", "consolidated"])]
    df.to_sql("fs", db, if_exists="append", index=False)
