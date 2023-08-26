import pandas as pd
from core.repository import maria_home


def get_stocks():
    return pd.read_sql("stocks", maria_home())


def find_stock(stock_code: str):
    stocks = get_stocks()
    corp_list = stocks[stocks["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return stocks[stocks["stock_code"] == stock_code].iloc[0]
