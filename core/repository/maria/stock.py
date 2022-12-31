from .conn import maria_home
import pandas as pd


def fetch_all() -> pd.DataFrame:
    return pd.read_sql("select * from stock", maria_home()).set_index("code")
