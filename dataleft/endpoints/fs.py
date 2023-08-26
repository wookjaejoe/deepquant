import numpy as np

from dataleft.app import app
from core.repository import FinanceLoader
from base.timeutil import YearQuarter
import logging

_logger = logging.getLogger()

_logger.info(f"Initializing {FinanceLoader.__name__}...")
_loader = FinanceLoader()
_logger.info(f"{FinanceLoader.__name__} initialized.")


@app.get("/fs")
def _fs(
    year: int,
    qtr: int
):
    result = _loader.load_by_qtr(YearQuarter(year, qtr))
    result = result.fillna(np.nan)
    result = result.replace(np.nan, None)
    return result.to_dict("records")
