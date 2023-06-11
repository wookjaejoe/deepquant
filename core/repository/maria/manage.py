from datetime import date, timedelta

import pandas as pd
from pandas import DataFrame
from pykrx.website.krx.market.core import 상장종목검색

from base.timeutil import YearMonth
from core.repository.krx import get_ohlcv_by_ticker
from core.repository.maria.conn import MariaConnection, maria_home

db = maria_home()


def create_chart_table(table_name: str):
    with MariaConnection() as conn:
        conn.query(
            f"""
            create table {table_name}
            (
                date        date       not null,
                code        varchar(8) not null,
                open        int        null,
                high        int        null,
                low         int        null,
                close       int        null,
                cap         bigint     null,
                vol         bigint     null,
                val         bigint     null,
                shares      bigint     null,
                primary key (code, date)
            );
            """
        )
        conn.commit()
        conn.query(f"create index chart_code_index on {table_name} (code);")
        conn.query(f"create index chart_date_index on {table_name} (date);")
        conn.commit()


def drop_table_if_exists(table_name: str):
    with MariaConnection() as conn:
        conn.query(f"drop table if exists {table_name}")


def update_chart_by_code(code: str, table_name: str):
    print(code)
    df = get_ohlcv_by_ticker(
        fromdate='20000101',
        todate=(date.today() - timedelta(days=1)).strftime('%Y%m%d'),
        ticker=code,
    )
    df["code"] = code
    df.set_index(["code", "date"], inplace=True)
    df.sort_index(inplace=True)
    df.to_sql(table_name, db, if_exists="append", index=True)


def update_chart(codes: list):
    todate = date.today().strftime('%Y%m%d')
    table_name = f"chart_{todate}"

    for code in codes:
        update_chart_by_code(code, table_name)

    drop_table_if_exists("chart")
    create_chart_table("chart")
    with MariaConnection() as conn:
        conn.query(f"insert into chart (select * from {table_name})")
        conn.commit()


def update_stocks() -> DataFrame:
    df = 상장종목검색().fetch()
    df = df[["short_code", "codeName", "marketName"]]
    df.columns = ["code", "name", "exchange"]

    table_name = "stock_" + date.today().strftime('%Y%m%d')
    df.to_sql(table_name, db, if_exists="replace", index=False)

    with MariaConnection() as conn:
        conn.query("drop table if exists stock")
        conn.query(f"create table stock as select * from {table_name}")

    return df


def upload_chart_from_krx():
    stocks = pd.read_sql("select * from stock", maria_home())
    update_chart(list(stocks["code"]))


def create_month_chart_table(table_name: str):
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


def upload_month_chart():
    todate = date.today().strftime('%Y%m%d')
    table_name = f"month_chart_{todate}"

    drop_table_if_exists(table_name)
    create_month_chart_table(table_name)

    dates = pd.read_sql("select distinct date from chart", maria_home())["date"]
    yms = sorted({YearMonth.from_date(d) for d in dates})
    assert len(yms) == len(set(yms))
    for ym in min(yms).to(max(yms)):
        print(ym)
        insert_month_chart(table_name, ym.year, ym.month)

    drop_table_if_exists("month_chart")
    create_month_chart_table("month_chart")
    with MariaConnection() as conn:
        conn.query(f"insert into month_chart (select * from {table_name})")
        conn.commit()
