from __future__ import annotations

import os
import sqlite3
from datetime import date

import pandas as pd

from repository import get_day_chart, get_bussness_months
from repository.maria import corp
import repository.deepsearch as ds


# noinspection DuplicatedCode
def get_next(before_date: date, after_date: date):
    before = get_day_chart(before_date)
    before = before[before['vol'] != 0]  # 거래량 미확인 종목 제외
    before = before[before['cap'] != 0]  # 시가총액 미확인 종목 제외
    # 매도일 주가데이터 조회
    after = get_day_chart(after_date)

    before_only = before.merge(after, how="outer", left_index=True, right_index=True).drop(after.index)
    if len(before_only) > 0:  # 상장폐지?
        pass

    # 팩터 계산
    df = before['close'].to_frame("before_close")
    df = df.join(before['cap'].to_frame('before_cap'))
    df = df.join(after['close'].to_frame('after_close'))
    df = df.join(after['cap'].to_frame('after_cap'))
    df = df.join(pd.Series([before_date] * len(before.index), index=before.index).to_frame("before_date"))
    df = df.join(pd.Series([after_date] * len(after.index), index=after.index).to_frame("after_date"))

    # **손익계산**
    # - 매출총이익(**G**ross **P**rofit)
    # - 영업이익(**O**perating **P**rofit)
    # - 당기순이익(**N**et **P**rofit)
    # **재무상태**
    # - 자산(**A**sset)
    # - 자본(**E**quity)
    # **현금흐름 ← 이건 어떻게 팩터에 적용할지 모르겠음…**
    # - 영업활동으로인한현금흐름(Cash Flows from Operating Activities): CF
    # - 투자활동으로인한현금흐름(Cash Flow from Investment Activities)
    # - 재무활동으로인한현금흐름(Cash Flow from Financial Activities)
    df = df.join(ds.load_and("매출총이익", before_date.year, before_date.month, 4, sum))
    df = df.join(ds.load_and("영업이익", before_date.year, before_date.month, 4, sum))
    df = df.join(ds.load_and("당기순이익", before_date.year, before_date.month, 4, sum))
    df = df.join(ds.load_one("자산", before_date.year, before_date.month))
    df = df.join(ds.load_one("자본", before_date.year, before_date.month))
    df['name'] = [corp.get_name(code) for code in df.index]
    return df


def run():
    db_file = ".out/_changes.db"
    if os.path.isfile(db_file):
        os.remove(db_file)

    con = sqlite3.connect(db_file)
    table_name = "changes"
    prev_date = None
    from_date = date(2001, 4, 1)
    to_date = date(2022, 10, 31)

    for this_date in get_bussness_months(from_date, to_date):
        print(this_date)
        if prev_date is None:
            prev_date = this_date
            continue

        df = get_next(prev_date, this_date)
        df.to_sql(table_name, con, if_exists="append")
        con.commit()
        prev_date = this_date

    con.close()


if __name__ == '__main__':
    run()
