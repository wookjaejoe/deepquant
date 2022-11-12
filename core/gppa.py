from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

import repository.deepsearch as ds
from repository import get_day_chart
from repository import rt
from .factor import QuantFactor


class GPPA(QuantFactor):
    name = "GP/PA"
    PW: float = 1.9
    AW: float = 0.9

    @classmethod
    def calc(cls, day: date = date.today()) -> pd.DataFrame:
        # fixme: nan 처리: sum([x.fillna(0) for x in ds.load("매출총이익", before_date.year, before_date.month, 4)])
        # 재무데이터 로드
        df = sum(ds.load_many("매출총이익", day.year, day.month, 4))
        df = pd.merge(df, ds.load_one("자산", day.year, day.month), left_index=True, right_index=True)

        # 시가총액 칼럼 추가
        if day == date.today():
            df = df.join(pd.Series({x.code: x.cap for x in rt.fetch_all()}).to_frame("cap"))
        else:
            df = df.join(get_day_chart(day)["cap"].to_frame("cap"))

        # GP/PA 계산
        df[cls.name] = df["매출총이익"] / ((df["cap"] ** cls.PW) * (df["자산"] ** cls.AW))
        df = df.replace([np.inf, -np.inf], np.nan)
        return df.sort_values(by=cls.name, ascending=False)
