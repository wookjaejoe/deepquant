import pandas as pd

from core.base import xox
from base.timeutil import YearQuarter
from . import deepsearch as ds
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
        ds.load_one("자산총계", year, month),
        ds.load_one("자본총계", year, month),
        ds.load_and_sum("매출액", year, month, 4),
        ds.load_and_sum("매출총이익", year, month, 4),
        ds.load_and_sum("영업이익", year, month, 4),
        ds.load_and_sum("당기순이익", year, month, 4),
        # ds.load_and_sum("영업활동으로인한현금흐름", year, month, 4),
    )

    result["GP_YoY"] = xox(ds.load_and_sum("매출총이익", year - 1, month, 4), result["매출총이익"])
    result["O_YoY"] = xox(ds.load_and_sum("영업이익", year - 1, month, 4), result["영업이익"])
    result["E_YoY"] = xox(ds.load_and_sum("당기순이익", year - 1, month, 4), result["당기순이익"])
    # result["CF_YoY"] = xox(ds.load_and_sum("영업활동으로인한현금흐름", year - 1, month, 4), result["영업활동으로인한현금흐름"])

    result["GP_QoQ"] = xox(ds.load_one("매출총이익", year - 1, month), ds.load_one("매출총이익", year, month))
    result["O_QoQ"] = xox(ds.load_one("영업이익", year - 1, month), ds.load_one("영업이익", year, month))
    result["E_QoQ"] = xox(ds.load_one("당기순이익", year - 1, month), ds.load_one("당기순이익", year, month))
    # result["CF_QoQ"] = xox(ds.load_one("영업활동으로인한현금흐름", year - 1, month), ds.load_one("영업활동으로인한현금흐름", year, month))

    result["확정실적"] = YearQuarter.last_confirmed(year, month)
    result.replace([np.inf, -np.inf], np.nan, inplace=True)
    return result
