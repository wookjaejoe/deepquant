from typing import *

import numpy as np
import pandas as pd

from base.timeutil import YearQuarter
from core.base.quantutil import xox
from core.repository.mongo import ds


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


FINANIAL_ALIAS = {
    "자산총계": "A",  # Asset
    "자본총계": "EQ",  # Equity
    "매출액": "R",  # Revenue
    "매출총이익": "GP",  # Gross Profit
    "영업이익": "O",  # Operating Income
    "당기순이익": "N",  # Net Income
}


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


def pre_load_financial_with_2022_4q():
    """
    ./core/repository/dartx/example1 과 함께 사용할 수 있는 함수.
    당장 사용할 일이 없음 2024년 초 쯤에 다시 한번 사용하게 될지도?
    """
    raw = pd.read_csv("2022-4Q.csv", dtype={"code": str}).set_index("code")
    result = merge(
        raw["당해년도_자산총계"].rename("자산총계"),
        raw["당해년도_자본총계"].rename("자본총계"),
        raw["당해년도_매출액"].rename("매출액"),
        raw["당해년도_영업이익"].rename("영업이익"),
    )

    result["R_QoQ"] = raw["매출액_QoQ"]
    result["R/A_QoQ"] = raw["매출액/자산총계_QoQ"]
    result["O_QoQ"] = raw["영업이익_QoQ"]
    result["O/A_QoQ"] = raw["영업이익/자산총계_QoQ"]

    result["확정실적"] = YearQuarter(2022, 4)
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
