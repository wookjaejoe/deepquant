from datetime import datetime

import numpy as np
from pandas import DataFrame
from pykrx.website.krx.market.core import 개별종목시세, 전종목시세
from pykrx.website.krx.market.ticker import get_stock_ticker_isin

from datetime import date, timedelta


def get_ohlcv_by_ticker(fromdate: str, todate: str, ticker: str, adjusted: bool = True) -> DataFrame:
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
        'vol', 'val', 'shares',
        # '등락률',
    ]
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, "%Y/%m/%d").date())
    df = df.replace(r'[^-\w\.]', '', regex=True)
    df = df.replace(r'\-$', '0', regex=True)
    df = df.replace('', '0')
    df = df.astype({
        "open": np.int32, "high": np.int32, "low": np.int32, "close": np.int32, 'cap': np.int64,
        "vol": np.int64, "val": np.int64, 'shares': np.int64,
        # "등락률": np.float32
    })
    return df.sort_index()


def get_ohlcv_by_date(date_: date):
    df = 전종목시세().fetch(date_.strftime("%Y%m%d"), "ALL")
    df = df[[
        "ISU_SRT_CD", "ISU_ABBRV",
        "TDD_OPNPRC", "TDD_HGPRC", "TDD_LWPRC", "TDD_CLSPRC", "MKTCAP", "MKT_NM",
        "FLUC_RT", "ACC_TRDVOL", "ACC_TRDVAL", "LIST_SHRS",
    ]]
    df.columns = [
        "code", "name",
        "open", "high", "low", "close", "cap", "exchange",
        "changesRatio", "vol", "val", "shares",
    ]
    df = df.replace(r"[^-\w\.]", "", regex=True)
    df = df.replace(r"\-$", "0", regex=True)
    df = df.replace("", "0")
    df = df.astype({
        "open": np.int32, "high": np.int32, "low": np.int32, "close": np.int32, "cap": np.int64,
        "vol": np.int64, "val": np.int64, "shares": np.int64,
    })
    df = df.sort_index()
    return df


def get_ohlcv_latest():
    for i in range(10):
        df = get_ohlcv_by_date(date.today() - timedelta(days=i))
        if len(df[df["vol"] != 0]) / len(df) > 0.95:
            return df
