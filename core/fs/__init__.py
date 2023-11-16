from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from core.repository import maria_home
from utils import pdutil
from utils.timeutil import YearQtr

AccAlias = {
    "자산총계": "A",  # Asset
    "자본총계": "EQ",  # Equity
    "매출": "R",  # Revenue
    "매출총이익": "GP",  # Gross Profit
    "영업이익": "O",  # Operating Income
    "당기순이익": "E",  # Net Income
    "법인세비용차감전계속영업이익": "EBT",
}


def load_ifrs():
    result = pd.read_sql(f"select * from ifrs", maria_home("finance"))
    result = result.rename(columns={
        "reportId": "report_id",
        "accountId": "account_id",
        "accountName": "account_name",
    })
    result["account_id"] = result["account_id"].astype(str)
    return result


class Growth:
    @staticmethod
    def rate(aft: pd.Series, pre: pd.Series) -> pd.Series:
        return (aft - pre) / abs(pre)


def select_consolidation(data: pd.DataFrame):
    if data[data["consolidated"] == 1].empty:
        return 0
    else:
        return 1


def due_date(settle_date: date, qtr: int):
    assert qtr in [1, 2, 3, 4]
    if qtr == 4:
        return settle_date + timedelta(days=90)
    else:
        return settle_date + timedelta(days=45)


class FsLoader:
    _fs_loader: FsLoader = None

    @classmethod
    def instance(cls):
        if not cls._fs_loader:
            cls._fs_loader = FsLoader()

        return cls._fs_loader

    def __init__(self):
        self.table = pd.read_sql(f"select * from fs", maria_home("finance"))
        self.table = self.table[self.table["qtr"] * 3 == self.table["month"]]
        self.table.fillna(np.nan, inplace=True)
        self.table.rename(columns={"date": "settle_date"}, inplace=True)
        self.table["due_date"] = self.table.apply(lambda row: due_date(row["settle_date"], row["qtr"]), axis=1)
        self.table.reset_index(drop=True, inplace=True)
        self.table = self.table[pdutil.sort_columns(
            self.table.columns,
            ["code", "settle_date", "due_date", "qtr", "consolidated"])
        ]
        self.table.sort_values("settle_date", ascending=False, inplace=True)

    def load(self, year: int, qtr: int, consolidated: int = None):
        """
        :param year: 조회 년도
        :param qtr: 조회 분기
        :param consolidated: 연결/별도 - None 인 경우 주 연결 재무제표 우선
        """

        assert qtr in [1, 2, 3, 4]
        yq = YearQtr(year, qtr)

        # [0]: 조회한 분기 데이터, [1]: 직전 분기 데이터, ... [4]: 4분기 전 데이터
        fins = [pdutil.find(self.table, year=yq.minus(i).year, qtr=yq.minus(i).qtr) for i in range(5)]

        # 연결/별도 결정
        if consolidated is None:
            indexer = fins[0].groupby(["code"]).apply(select_consolidation)
        else:
            indexer = fins[0].groupby(["code"]).apply(lambda x: consolidated)

        indexer = pd.MultiIndex.from_frame(indexer.to_frame("consolidated").reset_index())
        fins = [fin.set_index(["code", "consolidated"]) for fin in fins]
        fins = [fin[fin.index.isin(indexer)].reset_index(level=1) for fin in fins]

        common_indices = set(fins[0].index)
        for fin in fins[1:]:
            common_indices = common_indices.intersection(set(fin.index))

        fins = [fin.loc[list(common_indices)] for fin in fins]

        result = pd.DataFrame(indexer.tolist(), columns=indexer.names).set_index("code")
        bs_cols = ["자산총계", "자본총계"]
        result[[AccAlias[col] for col in bs_cols]] = fins[0][bs_cols]

        result["확정실적"] = yq

        is_cols = ["매출", "매출총이익", "영업이익", "법인세비용차감전계속영업이익", "당기순이익"]

        for is_col in is_cols:
            col_acc = AccAlias[is_col]

            # 4분기 누적 이익
            result[f"{AccAlias[is_col]}/Y"] = pd.concat([fins[i][is_col].rename(i) for i in range(4)], axis=1).sum(axis=1)

            # 매출 이익율
            if col_acc != "R":
                result[f"{col_acc}/R"] = result[f"{col_acc}/Y"] / result["R/Y"]

            # 자본, 자산 이익률
            result[f"{col_acc}/EQ"] = result[f"{col_acc}/Y"] / fins[1]["자본총계"]
            result[f"{col_acc}/A"] = result[f"{col_acc}/Y"] / fins[1]["자산총계"]

            # 이익 QoQ
            result[f"{AccAlias[is_col]}_QoQ"] = Growth.rate(fins[0][is_col], fins[4][is_col])
            result[f"{AccAlias[is_col]}/EQ_QoQ"] = fins[0][is_col] / result["EQ"] - fins[4][is_col] / result["EQ"]
            result[f"{AccAlias[is_col]}/A_QoQ"] = fins[0][is_col] / result["A"] - fins[4][is_col] / result["A"]

        return result
