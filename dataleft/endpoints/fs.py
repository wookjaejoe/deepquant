import logging

import numpy as np

from core.repository import FinanceLoader
from dataleft.app import app
from utils.timeutil import YearQtr

_logger = logging.getLogger()

_logger.info(f"Initializing {FinanceLoader.__name__}...")
_loader = FinanceLoader()
_logger.info(f"{FinanceLoader.__name__} initialized.")


@app.get("/fs")
def _fs(
    year: int,
    qtr: int,
    start: int,
    end: int
):
    result = _loader.load_by_qtr(YearQtr(year, qtr))
    result = result.fillna(np.nan)
    result = result.replace(np.nan, None)
    result = result.reset_index().dropna().to_dict("records")
    # todo: inf 조심
    return result[start:end]
