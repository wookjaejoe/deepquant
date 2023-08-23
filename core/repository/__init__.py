import pandas as pd

from base.timeutil import YearQuarter
from core.repository.maria.conn import maria_home
from core.repository.mongo import ds
from core.repository.dartx.corps import stocks

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


def prioritize_cfs(data: pd.DataFrame):
    # fixme: 분기별 CFS 우선. 재귀로 해결해야할듯 data.groupby(["bsns_year", "reprt_code"]).apply(prioritize_cfs)
    cfs = data[data["fs_div"] == "CFS"]
    if cfs.empty:
        return data[data["fs_div"] == "OFS"]
    else:
        return cfs


class FinanceLoader:
    def __init__(self):
        """
        1분기보고서 : 11013
        반기보고서 : 11012
        3분기보고서 : 11014
        사업보고서 : 11011
        """
        # self._table = pd.read_sql(f"select * from finance", maria_home()).set_index("code")
        self._table = pd.read_sql(f"select * from fnlttSinglAcntAll", maria_home()).set_index("corp_code")
        self._table["quarter"] = self._table["reprt_code"].replace({
            "11013": 1,
            "11012": 2,
            "11014": 3,
            "11011": 4
        })
        self._table = self._table.merge(stocks, left_on="corp_code", right_on="corp_code")
        self._table = self._table.rename(columns={"stock_code": "code"})
        self._table = self._table.groupby(["code"]).apply(prioritize_cfs)
        self._table = self._table.reset_index(drop=True)
        self._table = self._table.rename(columns={
            "bsns_year": "year"
        })
        self._table = self._table.set_index("code")
        # fixme: __init__ 안의 코드는 데이터 수집이 끝난 이후 변경될 예정

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
            result[f"{FinanceAlias[col]}_QoQ"] = Growth.rate(fins[0][col], fins[4][col])

        for col in is_cols:
            result[f"{FinanceAlias[col]}_QoQA"] = (Growth.rate(fins[0][col], fins[4][col]) -
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
