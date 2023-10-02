from datetime import date

import pandas as pd
import pykrx

from core.repository.krx import get_ohlcv_by_date
from core.repository.maria.conn import MariaConnection, maria_home
from utils import pdutil


def update_chart(fromdate: date):
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
        df.to_sql("chart2", db, if_exists="append", index=True)


def _create_month_chart_table(table_name: str):
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


def insert_month_chart(table_name: str, year: int, month: int):
    with MariaConnection() as conn:
        conn.query(f"""insert into {table_name} (
        SELECT
            code,
            MAX(date)                                                                    as date,
            cast(SUBSTRING_INDEX(MIN(CONCAT(date, '_', open)), '_', -1) as unsigned)     as open,
            cast(MAX(high) as unsigned)                                                  as high,
            cast(MIN(low) as unsigned)                                                   as low,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', close)), '_', -1) as unsigned)    as close,
            cast(SUM(vol) as unsigned)                                                   as vol,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', vol)), '_', -1) as unsigned)      as vol_last,
            cast(SUM(val) as unsigned)                                                   as val,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', val)), '_', -1) as unsigned)      as val_last,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', cap)), '_', -1) as unsigned)      as cap,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', shares)), '_', -1) as unsigned)   as shares
        FROM chart
        where year(date) = {year} and month(date) = {month}
        group by code, year(date), month(date)
        order by date);
        """)
        conn.commit()


def update_index_tickers(fromdate: str = "19600101"):
    todate = date.today().strftime('%Y%m%d')

    tickers = ["1001", "2001"]
    for ticker in tickers:
        db = maria_home("finance")
        df = pykrx.stock.get_index_ohlcv(
            fromdate=fromdate,
            todate=todate,
            ticker=ticker
        )
        df["ticker"] = ticker
        df["date"] = df.index
        df["date"] = df["date"].dt.date
        df = df[pdutil.sort_columns(df.columns, ["ticker", "date"])]
        df.to_sql("index_chart", db, index=False, if_exists="append")
