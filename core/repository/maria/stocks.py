import pandas as pd
from core.repository.maria.conn import maria_home


def get_stocks():
    """
    취급하는 주식 종목 반환
    """
    stocks = pd.read_sql("stocks", maria_home())
    stocks = stocks[~stocks["stock_name"].str.contains("스팩")]  # 스펙제거
    stocks = stocks[~stocks["stock_name"].str.match(r".*\d+호$")]  # ~N호 제거
    stocks = stocks[stocks["stock_code"].str.endswith("0")]  # 우선주 등 제외
    return stocks


def find_stock(stock_code: str):
    stocks = get_stocks()
    corp_list = stocks[stocks["stock_code"] == stock_code]
    if len(corp_list) > 0:
        return stocks[stocks["stock_code"] == stock_code].iloc[0]


def find_corp(corp_code: str):
    stocks = get_stocks()
    corp_list = stocks[stocks["corp_code"] == corp_code]
    if len(corp_list) > 0:
        return stocks[stocks["corp_code"] == corp_code].iloc[0]
