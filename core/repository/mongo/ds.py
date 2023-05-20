import logging

import pandas as pd

from core.repository.mongo import DsCollection

_logger = logging.getLogger(__name__)


def _load(raw: dict, title: str) -> pd.Series:
    content = raw['data']['pods'][1]['content']
    data = content['data']
    columns = content['columns']
    col = [col for col in columns if title in col][0]
    result = pd.Series(
        data[col],
        index=[symbol.split(":")[1] for symbol in data['symbol']],
        name=title
    )
    return result


def load_by_quarter(title: str, year: int, quarter: int) -> pd.Series:
    return _load(DsCollection.fetch_one(title, year, quarter), title)
