from datetime import datetime

import numpy as np
from pandas import DataFrame
from pykrx.website.krx.market.core import 개별종목시세, 전종목시세
from pykrx.website.krx.market.ticker import get_stock_ticker_isin

from datetime import date, timedelta
from utils import pdutil

_KRX_TABLE_COLUMNS = {
    "TRD_DD": "date",
    "ISU_SRT_CD": "code",
    "ISU_CD": "_ISU_CD",
    "ISU_ABBRV": "name",
    "MKT_NM": "market_name",
    "SECT_TP_NAME": "sector_type",
    "TDD_CLSPRC": "close",
    "FLUC_TP_CD": "_FLUC_TP_CD",
    "CMPPREVDD_PRC": "_CMPPREVDD_PRC",
    "FLUC_RT": "_FLUC_RT",
    "TDD_OPNPRC": "open",
    "TDD_HGPRC": "high",
    "TDD_LWPRC": "low",
    "ACC_TRDVOL": "vol",
    "ACC_TRDVAL": "val",
    "MKTCAP": "cap",
    "LIST_SHRS": "shares",
    "MKT_ID": "_MKT_ID"
}

DATA_TYPES = {
    "open": np.int32,
    "high": np.int32,
    "low": np.int32,
    "close": np.int32,
    "cap": np.int64,
    "vol": np.int64,
    "val": np.int64,
    "shares": np.int64,
}


def resolve_table(df: DataFrame):
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    return df.astype(DATA_TYPES)


def get_ohlcv_by_ticker(fromdate: str, todate: str, ticker: str, adjusted: bool = False) -> DataFrame:
    isin = get_stock_ticker_isin(ticker)
    adjusted = 2 if adjusted else 1
    df = 개별종목시세().fetch(fromdate, todate, isin, adjusted)
    df = df.rename(columns=_KRX_TABLE_COLUMNS)
    df["code"] = ticker
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, "%Y/%m/%d").date())
    df = resolve_table(df)
    return df[pdutil.sort_columns(df.columns, ["date", "code", "close"])]


def get_ohlcv_by_date(target_date: date):
    df = 전종목시세().fetch(target_date.strftime("%Y%m%d"), "ALL")
    df = df.rename(columns=_KRX_TABLE_COLUMNS)
    df["date"] = target_date
    df = resolve_table(df)
    return df[pdutil.sort_columns(df.columns, ["date", "code", "name", "close"])]


def get_ohlcv_latest():
    for i in range(14):
        target_date = date.today() - timedelta(days=i)

        if target_date.weekday() in [5, 6]:
            # 토, 일 제외
            continue

        df = get_ohlcv_by_date(target_date)
        if len(df[df["vol"] != 0]) / len(df) > 0.95:
            return df
