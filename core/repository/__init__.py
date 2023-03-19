import pandas as pd

from core.base.quantutil import xox
from base.timeutil import YearQuarter
from .deepsearch.loader import load_one, load_and_sum
import numpy as np


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

    result["R_YoY"] = xox(load_and_sum("매출액", year - 1, month, 4), result["매출액"])
    result["GP_YoY"] = xox(load_and_sum("매출총이익", year - 1, month, 4), result["매출총이익"])
    result["O_YoY"] = xox(load_and_sum("영업이익", year - 1, month, 4), result["영업이익"])
    result["E_YoY"] = xox(load_and_sum("당기순이익", year - 1, month, 4), result["당기순이익"])

    result["R_QoQ"] = xox(load_one("매출액", year - 1, month), load_one("매출액", year, month))
    result["GP_QoQ"] = xox(load_one("매출총이익", year - 1, month), load_one("매출총이익", year, month))
    result["O_QoQ"] = xox(load_one("영업이익", year - 1, month), load_one("영업이익", year, month))
    result["E_QoQ"] = xox(load_one("당기순이익", year - 1, month), load_one("당기순이익", year, month))

    def profit_ratio(profit: str, based: str):
        aft = load_one(profit, year, month) / load_one(based, year, month)
        bef = load_one(profit, year - 1, month) / load_one(based, year - 1, month)
        return aft - bef

    result["R/A_QoQ"] = profit_ratio("매출액", "자산총계")
    result["GP/A_QoQ"] = profit_ratio("매출총이익", "자산총계")
    result["O/A_QoQ"] = profit_ratio("영업이익", "자산총계")
    result["E/A_QoQ"] = profit_ratio("당기순이익", "자산총계")

    result["R/EQ_QoQ"] = profit_ratio("매출액", "자본총계")
    result["GP/EQ_QoQ"] = profit_ratio("매출총이익", "자본총계")
    result["O/EQ_QoQ"] = profit_ratio("영업이익", "자본총계")
    result["E/EQ_QoQ"] = profit_ratio("당기순이익", "자본총계")

    result["확정실적"] = YearQuarter.last_confirmed(year, month)
    result.replace([np.inf, -np.inf], np.nan, inplace=True)

    return result


def pre_load_financial_with_2022_4q():
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
