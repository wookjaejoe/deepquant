"""
상장 폐지 기업 조회
"""

import os
import pandas as pd


def get_delist_stocks():
    """
    columns: 회사명, 종목코드, 폐지일자, 폐지사유, 비고
    """
    file = os.path.join(os.path.dirname(__file__), "delist.csv")
    df = pd.read_csv(file)
    df["종목코드"] = df["종목코드"].apply(lambda code: str(code).rjust(6, "0"))
    return pd.read_csv(file)
