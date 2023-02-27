from datetime import datetime, date, timedelta
import pandas as pd

import numpy as np
from pandas import DataFrame
from pykrx.website.krx.market.core import 개별종목시세, 상장종목검색
from pykrx.website.krx.market.ticker import get_stock_ticker_isin

from core.repository.maria.conn import maria_home, MariaConnection

db = maria_home()


def get_market_ohlcv_by_date(fromdate: str, todate: str, ticker: str, adjusted: bool = True) -> DataFrame:
    isin = get_stock_ticker_isin(ticker)
    adjusted = 2 if adjusted else 1
    df = 개별종목시세().fetch(fromdate, todate, isin, adjusted)

    df = df[[
        'TRD_DD',
        'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'MKTCAP',
        'ACC_TRDVOL', 'ACC_TRDVAL', 'LIST_SHRS',
        # 'FLUC_RT',
    ]]
    df.columns = [
        'date',
        'open', 'high', 'low', 'close', 'cap',
        'vol', 'amount', 'list_shares',
        # '등락률',
    ]
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, "%Y/%m/%d").date())
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.astype({
        "open": np.int32, "high": np.int32, "low": np.int32, "close": np.int32, 'cap': np.int64,
        "vol": np.int64, "amount": np.int64, 'list_shares': np.int64,
        # "등락률": np.float32
    })
    return df.sort_index()


def create_chart_table(table_name: str):
    with MariaConnection() as conn:
        conn.query(
            f"""
            create table finance.{table_name}
            (
                date        date       not null,
                code        varchar(8) not null,
                open        int        null,
                high        int        null,
                low         int        null,
                close       int        null,
                cap         bigint     null,
                vol         bigint     null,
                amount      bigint     null,
                list_shares bigint     null,
                primary key (date, code)
            );
            """
        )


def drop_table_if_exists(table_name: str):
    with MariaConnection() as conn:
        conn.query(f"drop table if exists {table_name}")


def update_chart(codes: list):
    todate = date.today().strftime('%Y%m%d')
    table_name = f"chart_{todate}"

    drop_table_if_exists(table_name)
    create_chart_table(table_name)

    count = 0
    for code in codes:
        count += 1
        print(f"[{count}/{len(codes)}] {code}")

        try:
            if len(pd.read_sql(f"select * from {table_name} where code = '{code}'", db)) > 0:
                continue
        except:
            pass

        df = get_market_ohlcv_by_date(
            fromdate='20000101',
            todate=(date.today() - timedelta(days=1)).strftime('%Y%m%d'),
            ticker=code,
        )
        df["code"] = code
        df.set_index(["date", "code"], inplace=True)
        df.sort_index(inplace=True)
        df.to_sql(table_name, db, if_exists="append", index=True)

    drop_table_if_exists("chart")
    create_chart_table("chart")
    with MariaConnection() as conn:
        conn.query(f"insert into chart (select * from {table_name})")
        conn.commit()
    print()


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


def main():
    stocks = update_stocks()
    update_chart(list(stocks["code"]))


if __name__ == '__main__':
    main()
