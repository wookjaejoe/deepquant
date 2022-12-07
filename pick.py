from datetime import date

import numpy as np
import pandas as pd

import repository.deepsearch as ds
from repository.maria import stock
from repository.rt import fetch_all

stocks = stock.fetch_all()

day = date(2022, 11, 30)
df = ds.load_and("매출총이익", day.year, day.month, 4, sum)
df = df.merge(ds.load_and("영업이익", day.year, day.month, 4, sum), left_index=True, right_index=True)
df = df.merge(ds.load_and("당기순이익", day.year, day.month, 4, sum), left_index=True, right_index=True)
df = df.merge(ds.load_one("자산", day.year, day.month), left_index=True, right_index=True)
df = df.merge(ds.load_one("자본", day.year, day.month), left_index=True, right_index=True)
df = df.merge(pd.DataFrame(fetch_all()).set_index("code"), left_index=True, right_index=True)
df = df.merge(stocks, left_index=True, right_index=True)
df.columns = df.columns.str.replace("cap", "시가총액")

factor = pow(sum([df["매출총이익"] / df["자산"], df["영업이익"] / df["자본"]]), 0.4) / df["시가총액"]
factor[df["매출총이익"] < 0] = np.nan
df = df.join(factor.to_frame("factor"))
df = df.join(df["factor"].rank(ascending=False, method="min").to_frame("rfactor"))

# fit scale to 1-1000, origin * target-sacle / origin-scale
df["rfactor"] = round(df["rfactor"] * 100 / max(df["rfactor"]))
df.to_csv("pick.csv")
