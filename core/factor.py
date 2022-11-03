from datetime import date
import pandas as pd


class QuantFactor:
    name = ...

    @classmethod
    def calc(cls, day: date) -> pd.DataFrame: ...
