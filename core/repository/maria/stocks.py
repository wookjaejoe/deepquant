import pandas as pd
from core.repository import maria_home


def get_stocks():
    """
    취급하는 주식 종목 반환
    """
    stocks = pd.read_sql("stocks", maria_home())
    stocks = stocks[stocks["corp_cls"] != 'E']
    stocks = stocks[~stocks["stock_name"].str.endswith("스팩")]
    stocks = stocks[stocks["stock_code"].str.endswith("0")]
    return stocks


stocks = get_stocks()


def find_stock(stock_code: str):
    corp_list = stocks[stocks["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return stocks[stocks["stock_code"] == stock_code].iloc[0]


def find_corp(corp_code: str):
    corp_list = stocks[stocks["corp_code"] == corp_code]
    if len(corp_list) > 0:
        return stocks[stocks["corp_code"] == corp_code].iloc[0]
