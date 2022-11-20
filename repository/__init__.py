from __future__ import annotations

import pandas as pd

from resources import get_resource
from .maria import chart


class Delisted:
    df: pd.DataFrame
    COLNAME_CODE = "종목코드"
    COLNAME_DATE = "폐지일자"

    @classmethod
    def reload(cls):
        cls.df = pd.read_csv(get_resource("상장폐지종목.csv"))
        cls.df[cls.COLNAME_CODE] = cls.df[cls.COLNAME_CODE].apply(lambda x: str(x).ljust(6, "0"))
        cls.df[cls.COLNAME_DATE] = pd.to_datetime(cls.df[cls.COLNAME_DATE], format="%Y-%m-%d")


Delisted.reload()
