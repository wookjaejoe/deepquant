from typing import *

import numpy as np
import pandas as pd

from base.timeutil import YearQuarter
from core.base.quantutil import xox
from core.repository.mongo import ds
from core.repository.maria.conn import maria_home

db = maria_home()


def merge(*data_list) -> pd.DataFrame:
    result = pd.DataFrame()
    for series in data_list:
        result = result.merge(
            series,
            left_index=True,
            right_index=True,
            how="outer"
        )

    return result


FinanceAlias = {
    "자산총계": "A",  # Asset
    "자본총계": "EQ",  # Equity
    "매출액": "R",  # Revenue
    "매출총이익": "GP",  # Gross Profit
    "영업이익": "O",  # Operating Income
    "당기순이익": "E",  # Net Income
}


class Growth:
    @staticmethod
    def rate(aft: pd.Series, pre: pd.Series) -> pd.Series:
        return (aft - pre) / abs(pre)


class FinanceLoader:
    def __init__(self):
        self._table = pd.read_sql(f"select * from finance", db).set_index("code")

    def _load_from_table(self, yq: YearQuarter):
        return self._table[(self._table["year"] == yq.year) & (self._table["quarter"] == yq.quarter)]

    def load(self, yq: YearQuarter):
        # [0]: 조회한 분기 데이터, [1]: 직전 분기 데이터, ... [5]: 5분기 전 데이터
        fins = [self._load_from_table(yq.minus(i)) for i in range(6)]
        result = pd.DataFrame()
        result["부채총계"] = fins[0]["자산총계"] - fins[0]["자본총계"]
        result["순유동자산"] = fins[0]["유동자산"] - result["부채총계"]
        result["부채비율"] = result["부채총계"] / fins[0]["자본총계"]
        result["자기자본비율"] = fins[0]["자본총계"] / fins[0]["자산총계"]
        result["A"] = fins[0]["자산총계"]
        result["EQ"] = fins[0]["자본총계"]

        # 손익계산서 4개분기 합
        is_cols = ["매출액", "매출총이익", "영업이익", "당기순이익"]
        for col in is_cols:
            df = pd.DataFrame()
            for i in range(4):
                df = df.merge(fins[i][col].rename(str(i)), how="outer", left_index=True, right_index=True)

            result[f"{FinanceAlias[col]}/Y"] = df.sum(axis=1)

        for col in is_cols:
            col_alias = FinanceAlias[col]
            result[f"{col_alias}_QoQ"] = Growth.rate(fins[0][col], fins[4][col])

        for col in is_cols:
            col_alias = FinanceAlias[col]
            result[f"{col_alias}_QoQA"] = (Growth.rate(fins[0][col], fins[4][col]) -
                                           Growth.rate(fins[1][col], fins[5][col]))

        bs_cols = ["자산총계", "자본총계"]
        for is_col in is_cols:
            for bs_col in bs_cols:
                name = f"{FinanceAlias[is_col]}/{FinanceAlias[bs_col]}_QoQ"
                result[name] = fins[0][is_col] / fins[0][bs_col] - fins[4][is_col] / fins[4][bs_col]

        # todo 언제시점 자본, 자산을 참조하는게 맞을까?
        # todo 성장가속 전분기대비
        # todo 등수 말고 포지션으로?
        return result


def load_financial(year, month) -> pd.DataFrame:
    result = merge(
        load_one("자산총계", year, month),
        load_one("자본총계", year, month),
        load_and_sum("매출액", year, month, 4),
        load_and_sum("매출총이익", year, month, 4),
        load_and_sum("영업이익", year, month, 4),
        load_and_sum("당기순이익", year, month, 4),
    )

    result["부채총계"] = result["자산총계"] - result["자본총계"]
    result["BIS"] = result["자본총계"] / result["자산총계"]
    result["직전자본총계"] = load_one("자산총계", year - 1, month)
    result["직전자산총계"] = load_one("자본총계", year - 1, month)
    result["BIS_QoQ"] = (result["자본총계"] / result["자산총계"]) - (result["직전자본총계"] / result["직전자산총계"])

    # YoY Series

    result["R_YoY"] = xox(load_and_sum("매출액", year - 1, month, 4), load_and_sum("매출액", year, month, 4))
    result["GP_YoY"] = xox(load_and_sum("매출총이익", year - 1, month, 4), load_and_sum("매출총이익", year, month, 4))
    result["O_YoY"] = xox(load_and_sum("영업이익", year - 1, month, 4), load_and_sum("영업이익", year, month, 4))
    result["E_YoY"] = xox(load_and_sum("당기순이익", year - 1, month, 4), load_and_sum("당기순이익", year, month, 4))

    def profit_yoy(profit: str, based: str):
        bef = load_and_sum(profit, year - 1, month, 4) / load_one(based, year - 1, month)
        aft = load_and_sum(profit, year, month, 4) / load_one(based, year, month)
        return aft - bef

    result["R/A_YoY"] = profit_yoy("매출액", "자산총계")
    result["GP/A_YoY"] = profit_yoy("매출총이익", "자산총계")
    result["O/A_YoY"] = profit_yoy("영업이익", "자산총계")
    result["E/A_YoY"] = profit_yoy("당기순이익", "자산총계")

    result["R/EQ_YoY"] = profit_yoy("매출액", "자본총계")
    result["GP/EQ_YoY"] = profit_yoy("매출총이익", "자본총계")
    result["O/EQ_YoY"] = profit_yoy("영업이익", "자본총계")
    result["E/EQ_YoY"] = profit_yoy("당기순이익", "자본총계")

    # QoQ Series

    result["R_QoQ"] = xox(load_one("매출액", year - 1, month), load_one("매출액", year, month))
    result["GP_QoQ"] = xox(load_one("매출총이익", year - 1, month), load_one("매출총이익", year, month))
    result["O_QoQ"] = xox(load_one("영업이익", year - 1, month), load_one("영업이익", year, month))
    result["E_QoQ"] = xox(load_one("당기순이익", year - 1, month), load_one("당기순이익", year, month))

    def profit_qoq(profit: str, based: str):
        bef = load_one(profit, year - 1, month) / load_one(based, year - 1, month)
        aft = load_one(profit, year, month) / load_one(based, year, month)
        return aft - bef

    result["R/A_QoQ"] = profit_qoq("매출액", "자산총계")
    result["GP/A_QoQ"] = profit_qoq("매출총이익", "자산총계")
    result["O/A_QoQ"] = profit_qoq("영업이익", "자산총계")
    result["E/A_QoQ"] = profit_qoq("당기순이익", "자산총계")

    result["R/EQ_QoQ"] = profit_qoq("매출액", "자본총계")
    result["GP/EQ_QoQ"] = profit_qoq("매출총이익", "자본총계")
    result["O/EQ_QoQ"] = profit_qoq("영업이익", "자본총계")
    result["E/EQ_QoQ"] = profit_qoq("당기순이익", "자본총계")

    result["확정실적"] = YearQuarter.last_confirmed(year, month)
    result.replace([np.inf, -np.inf], np.nan, inplace=True)

    return result


def load_by_quarter(title: str, year: int, quarter: int) -> pd.Series:
    try:
        return ds.load_by_quarter(title=title, year=year, quarter=quarter)
    except:
        return pd.read_csv(f"{year}-{quarter}Q.csv", dtype={"code": str}).set_index("code")[title]


def load_one(title: str, year: int, month: int) -> pd.Series:
    yq = YearQuarter.last_confirmed(year, month)
    return load_by_quarter(title, yq.year, yq.quarter)


def load_many(title: str, year: int, month: int, num: int) -> List[pd.Series]:
    last = YearQuarter.last_confirmed(year, month)
    return [load_by_quarter(title, yq.year, yq.quarter) for yq in [last.minus(i) for i in range(num)]]


def load_and_sum(title: str, year: int, month: int, num: int) -> pd.Series:
    df = pd.DataFrame()
    count = 0
    for one in load_many(title, year, month, num):
        df = df.merge(one.rename(f"{one.name}_{count}"), how="outer", left_index=True, right_index=True)
        count += 1

    # fixme: na를 그냥 drop 시키면 종목 누락인데, fillna 합리적으로 할 수 있는 방법을 찾아봐야 함.
    result = df.dropna().sum(axis=1)
    result.name = title
    return result


def load_and(
        title: str, year: int, month: int, num: int,
        operator: Callable[[List[pd.Series]], pd.Series],
) -> pd.Series:
    return operator(load_many(title, year, month, num))
