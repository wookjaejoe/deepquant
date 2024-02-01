"""
사용 예시)

from datetime import date
from core.repository.maria.manage import *

fromdate = date(2023, 11, 1)

clear(fromdate)
update_index_chart(fromdate)
update_chart(fromdate)
insert_month_chart(2023, 11)
insert_month_chart(2023, 12)
"""

from datetime import date

import pandas as pd
import pykrx

from core.repository.krx import get_ohlcv_by_date
from core.repository.maria.conn import MariaConnection, maria_home
from utils import pdutil
from sqlalchemy import text


def clear(fromdate: date):
    """
    지수, 일봉, 월봉 차트 데이터 삭제
    """

    print(f"Clearing index_chart, chart, month_chart where date >= {fromdate}")
    with MariaConnection() as conn:
        conn.query(f"delete from index_chart where date >= '{fromdate}';")
        conn.query(f"delete from chart where date >= '{fromdate}';")
        conn.query(f"delete from month_chart where date >= '{fromdate}';")
        conn.commit()


def update_index_chart(fromdate: date):
    """
    지수 데이터 수집
    """
    todate = date.today().strftime('%Y%m%d')

    tickers = ["1001", "2001"]
    for ticker in tickers:
        db = maria_home("finance")
        df = pykrx.stock.get_index_ohlcv(
            fromdate=fromdate.strftime('%Y%m%d'),
            todate=todate,
            ticker=ticker
        )
        df["ticker"] = ticker
        df["date"] = df.index
        df["date"] = df["date"].dt.date
        df = df[pdutil.sort_columns(df.columns, ["ticker", "date"])]

        print(f"Inserting into index_chart {ticker, fromdate, todate}")
        df.to_sql("index_chart", db, index=False, if_exists="append")


def update_chart(fromdate: date):
    """
    일봉 차트 데이터 수집

    지수 차트를 기반으로 개별 종목 일봉 차트 수집 날짜를 결정하기 때문에 지수 차트 수집이 선행되어야 함.
    """
    db = maria_home()
    dates = pd.read_sql("select distinct date from index_chart", maria_home("finance"))["date"]
    dates = [d for d in dates if d >= fromdate]
    num = 0
    for target_date in dates:
        num += 1
        print(f"[{num}/{len(dates)}]", target_date)
        df = get_ohlcv_by_date(target_date)
        df = df[[col for col in df.columns if not col.startswith("_")]]
        df = df.set_index(["code", "date"]).sort_index()
        df.to_sql("chart", db, if_exists="append", index=True)


def _create_month_chart_table(table_name: str):
    """
    월봉 차트 테이블 생성
    """
    with MariaConnection() as conn:
        conn.query(
            f"""
            create table {table_name}
            (
                code        varchar(8) null,
                date        date null,
                open        int null,
                high        int null,
                low         int null,
                close       int null,
                vol         bigint null,
                vol_last    bigint null,
                val         bigint null,
                val_last    bigint null,
                cap         bigint null,
                shares      bigint null,
                primary key (code, date)
            );
            """
        )
        conn.commit()
        conn.query("create index if not exists chart_code_index on month_chart (code);")
        conn.query("create index if not exists chart_date_index on month_chart (date);")
        conn.commit()


def insert_month_chart(year: int, month: int):
    """
    일봉 차트 테이블을 기반으로 월봉 차트 삽입
    """
    print(f"Inserting into month_chart {year}/{month}")
    db = maria_home("finance")
    with db.connect() as conn:
        conn.execute(text(f"""insert into month_chart (
        SELECT code,
            MAX(date)                                                    as date,
            SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY date), ',', -1)   as name,
            SUBSTRING_INDEX(GROUP_CONCAT(open ORDER BY date), ',', 1)    as open,
            MAX(high)                                                    as high,
            MIN(low)                                                     as low,
            SUBSTRING_INDEX(GROUP_CONCAT(close ORDER BY date), ',', -1)  as close,
            IFNULL(cast(SUM(val) / NULLIF(SUM(vol), 0) as unsigned), 0)  as avg,
            SUM(vol)                                                     as vol,
            SUBSTRING_INDEX(GROUP_CONCAT(vol ORDER BY date), ',', -1)    as vol_last,
            SUM(val)                                                     as val,
            SUBSTRING_INDEX(GROUP_CONCAT(val ORDER BY date), ',', -1)    as val_last,
            SUBSTRING_INDEX(GROUP_CONCAT(cap ORDER BY date), ',', -1)    as cap,
            SUBSTRING_INDEX(GROUP_CONCAT(shares ORDER BY date), ',', -1) as shares
        FROM chart
        where year(date) = {year} and month(date) = {month}
        group by code, year(date), month(date)
        order by date);
        """))
        conn.commit()
