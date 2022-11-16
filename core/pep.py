from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

import repository.deepsearch as ds
from repository import get_day_chart
from repository import rt
from .factor import QuantFactor


class PEP(QuantFactor):
    name = "PEP"

    @classmethod
    def calc(
        cls,
        day: date = date.today()
    ) -> pd.DataFrame:
        # 재무데이터 로드
        df = ds.load_and("매출총이익", day.year, day.month, 4, sum)
        df = df.merge(ds.load_and("영업이익", day.year, day.month, 4, sum), left_index=True, right_index=True)
        df = df.merge(ds.load_one("자산", day.year, day.month), left_index=True, right_index=True)
        df = df.merge(ds.load_one("자본", day.year, day.month), left_index=True, right_index=True)

        # 시가총액 칼럼 추가
        if day == date.today():
            df = df.join(pd.Series({x.code: x.cap for x in rt.fetch_all()}).to_frame("cap"))
        else:
            df = df.join(get_day_chart(day)["cap"].to_frame("cap"))

        df[cls.name] = pow(df["매출총이익"], 0.125) / pow(df["자산"], 0.125) / df["cap"]
        df = df.replace([np.inf, -np.inf], np.nan)
        return df.sort_values(by=cls.name, ascending=False)
