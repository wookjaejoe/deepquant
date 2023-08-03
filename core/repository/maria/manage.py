import traceback
from datetime import date, timedelta

import pandas as pd
from pandas import DataFrame
from pykrx.website.krx.market.core import 상장종목검색

from base.timeutil import YearMonth
from core.repository.krx import get_ohlcv_by_ticker
from core.repository.maria.conn import MariaConnection, maria_home

db = maria_home()


def drop_table_if_exists(table_name: str):
    with MariaConnection() as conn:
        conn.query(f"drop table if exists {table_name}")


def collect_chart(codes: list[str], table_name: str, fromdate: str, todate: str):
    for code in codes:
        print(code)
        try:
            df = get_ohlcv_by_ticker(
                fromdate=fromdate,
                todate=todate,
                ticker=code,
                adjusted=False
            )
            df["code"] = code
            df = df.set_index(["code", "date"]).sort_index()
            df.to_sql(table_name, db, if_exists="append", index=True)
        except KeyboardInterrupt as e:
            raise e
        except BaseException as e:
            traceback.print_exc()


def upload_chart(codes: list):
    print("Starting to update chart...")

    todate = date.today().strftime('%Y%m%d')
    table_name = f"chart_{todate}"

    try:
        excludes = list(pd.read_sql(f"select distinct code from {table_name}", maria_home())["code"])
    except:
        excludes = []

    codes = [code for code in codes if code not in excludes]

    collect_chart(
        codes, table_name,
        fromdate="20000101",
        todate=(date.today() - timedelta(days=1)).strftime('%Y%m%d')
    )

    drop_table_if_exists("chart")

    with MariaConnection() as conn:
        conn.query(f"CREATE TABLE chart LIKE {table_name};")
        conn.query(f"INSERT INTO chart SELECT * FROM {table_name};")
        conn.commit()

    # todo: make primary key (code, date)


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
    upload_chart(list(stocks["code"]))


def _update_chart_by_code(code: str, fromdate: date, todate: date):
    fromdate = fromdate.strftime('%Y%m%d')
    todate = todate.strftime('%Y%m%d')
    df = get_ohlcv_by_ticker(
        fromdate=fromdate,
        todate=todate,
        ticker=code,
        adjusted=False
    )
    df["code"] = code
    df = df.set_index(["code", "date"]).sort_index()
    df.to_sql("chart", db, if_exists="append", index=True)


def update_chart(fromdate: date, black_codes: list[str]):
    stocks = pd.read_sql("select * from stock", db)
    codes = stocks["code"]
    num = 0
    fromdatestr = fromdate.strftime("%Y-%m-%d")
    excludes = pd.read_sql(f"select distinct code from chart where date > '{fromdatestr}'", db)["code"].to_list()
    for code in codes:
        num += 1
        print(f"[{num}/{len(codes)}]", code)

        if code in excludes:
            print("Skip")
            continue

        if code in black_codes:
            print("Skip")
            continue

        # fixme: 해당 종목에 대한 레코드가 전혀 존재하지 않으면 새로 추가된 종목일 수 있으니, 해당 종목에 한하여 전체 기간 데이터 수집
        # _update_chart_by_code(code, fromdate, date.today() - timedelta(days=1))
        _update_chart_by_code(code, fromdate, date.today() + timedelta(days=1))


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


def upload_month_chart():
    todate = date.today().strftime('%Y%m%d')
    table_name = f"month_chart_{todate}"

    drop_table_if_exists(table_name)
    _create_month_chart_table(table_name)

    dates = pd.read_sql("select distinct date from chart", maria_home())["date"]
    yms = sorted({YearMonth.from_date(d) for d in dates})
    assert len(yms) == len(set(yms))
    for ym in min(yms).to(max(yms)):
        print(ym)
        insert_month_chart(table_name, ym.year, ym.month)

    drop_table_if_exists("month_chart")
    _create_month_chart_table("month_chart")
    with MariaConnection() as conn:
        conn.query(f"insert into month_chart (select * from {table_name})")
        conn.commit()
