import pandas as pd

from base.timeutil import YearQuarter
from core.repository.maria.conn import maria_home
from core.repository.mongo import ds

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
        self._table = pd.read_sql(f"select * from finance", maria_home()).set_index("code")

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
