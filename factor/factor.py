from datetime import date
import pandas as pd


class QuantFactor:
    ver = ...

    @classmethod
    def calc(cls, day: date) -> pd.DataFrame: ...
