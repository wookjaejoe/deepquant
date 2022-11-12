import pandas as pd
from .time import YearMonth, Quarter


def normalize(series: pd.Series, based_zero: bool = False) -> pd.Series:
    result = (series - series.mean()) / series.std()
    if based_zero:
        result = result + abs(min(result))

    return result


def cagr(initial: float, final: float, duration: float):
    return (final / initial) ** (1 / duration) - 1


def rate_of_return(initial: float, final: float):
    return (final - initial) / initial


def N(target: str):
    return of("N", target)


def R(target: str):
    return of("R", target)


def of(operation: str, subject: str):
    return f"{operation}({subject})"
