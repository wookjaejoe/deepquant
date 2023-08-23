import pandas as pd
from typing import *


def sort_columns(
    columns: List[str],
    forward: List[str] = None,
    backward: List[str] = None
):
    forward = [] if forward is None else forward
    backward = [] if backward is None else backward
    return forward + [c for c in columns if c not in forward + backward] + backward


def find(df: pd.DataFrame, **kwargs):
    for k, v in kwargs.items():
        df = df[df[k] == v]

    return df
