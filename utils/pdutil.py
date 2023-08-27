import pandas as pd
from typing import *
import io
import base64


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


def serialize(df: pd.DataFrame) -> str:
    """
    DataFrame 객체를 문자열로 직렬화
    DataFrame -> Pickle -> Base64
    """
    buffer = io.BytesIO()
    df.to_pickle(buffer)
    return base64.b64encode(buffer.getvalue()).decode("utf8")  # 이걸 base64 저장


def deserialize(b64: str) -> pd.DataFrame:
    """
    Base64 문자열을 DataFrame 객체로 변환
    Base64 -> Pickle -> DataFrame
    """
    decoded_bytes = base64.b64decode(b64)
    buffer = io.BytesIO(decoded_bytes)
    return pd.read_pickle(buffer)
