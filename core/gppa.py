from __future__ import annotations

from datetime import date

import pandas as pd

import repository.deepsearch as ds
from repository import get_day_chart
from repository import rt
from .factor import QuantFactor
import numpy as np
from util import R, N, normalize


class Gppa(QuantFactor):
    name = "GP/PA"
    PW: float = 1.9
    AW: float = 0.9

    @classmethod
    def calc(cls, day: date = date.today()) -> pd.DataFrame:
        # 재무데이터 로드
        df = sum(ds.load("매출액", day.year, day.month, 4))
        df = pd.merge(df, sum(ds.load("매출원가", day.year, day.month, 4)), left_index=True, right_index=True)
        df = pd.merge(df, ds.load_one("자산", day.year, day.month), left_index=True, right_index=True)

        # 시가총액 칼럼 추가
        if day == date.today():
            df = df.join(pd.Series({x.code: x.cap for x in rt.fetch_all()}).to_frame("cap"))
        else:
            df = df.join(get_day_chart(day)["cap"].to_frame("cap"))

        # GP/PA 계산
        df["매출총이익"] = df["매출액"] - df["매출원가"]
        df.loc[df["매출액"] < 0, "매출총이익"] = np.nan
        df[cls.name] = df["매출총이익"] / ((df["cap"] ** cls.PW) * (df["자산"] ** cls.AW))
        df = df.replace([np.inf, -np.inf], np.nan)
        return df.sort_values(by=cls.name, ascending=False)
