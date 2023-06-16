import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from queue import Queue
from threading import Thread

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
                code        varchar(8) not null,
                date        date       not null,
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


class ChartTableGenerator:
    def __init__(self, table_name):
        self.queue = Queue()
        self.table_name = table_name
        self.fromdate = '20000101'
        self.todate = (date.today() - timedelta(days=1)).strftime('%Y%m%d')

    def _consume_queue(self):
        while True:
            df = self.queue.get()
            if df is None:
                break

            try:
                df.to_sql(self.table_name, db, if_exists="append", index=True)
            except:
                traceback.print_exc()

    def _start_consume(self):
        queue_consumer = Thread(target=self._consume_queue)
        queue_consumer.start()

    def _stop_consume(self):
        self.queue.put(None)

    # @retry(tries=5, delay=10)
    def _fetch_ohlcv(self, code):
        print(code)
        try:
            df = get_ohlcv_by_ticker(
                fromdate=self.fromdate,
                todate=self.todate,
                ticker=code,
                adjusted=False
            )
            df["code"] = code
            df = df.set_index(["code", "date"]).sort_index()
            self.queue.put(df)
        except KeyboardInterrupt as e:
            raise e
        except BaseException as e:
            traceback.print_exc()

    def run(self, codes: list):
        self._start_consume()

        try:
            with ThreadPoolExecutor(max_workers=8) as executor:
                # 각 아이템에 대해 run 함수를 호출하여 병렬 처리
                executor.map(self._fetch_ohlcv, codes)
        finally:
            while not self.queue.empty():
                time.sleep(1)

            print(f"Queue empty: {self.queue.empty()}")
            print("Stop comsuming...")
            self._stop_consume()


def update_chart(codes: list):
    print("Starting to update chart...")

    todate = date.today().strftime('%Y%m%d')
    table_name = f"chart_{todate}"

    try:
        excludes = list(pd.read_sql(f"select distinct code from {table_name}", maria_home())["code"])
    except:
        excludes = []

    codes = [code for code in codes if code not in excludes]

    ChartTableGenerator(table_name).run(codes)

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
