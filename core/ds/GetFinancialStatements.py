# https://help.deepsearch.com/dp/api/func/company/financial/getfinancialstatements

import logging
from datetime import date
from typing import *

import pandas as pd

from core import ds

_logger = logging.getLogger()


def _call(
    entities: str | List[str],
    consolidated: bool,
    report_type="IFRS",
    is_annual=False,
    is_accumulated=False,
    report_ids=None,
    date_from=None,
    date_to=None
):
    if isinstance(entities, list):
        entities = ",".join(entities)

    report_ids = ",".join(report_ids) if report_ids is not None else None
    return ds.call(
        "GetFinancialStatements",
        entities,
        report_type=f"\"{report_type}\"",
        consolidated=consolidated,
        is_annual=is_annual,
        is_accumulated=is_accumulated,
        report_ids=report_ids,
        date_from=date_from,
        date_to=date_to
    )


def call(
    code: str,
    consolidated: bool,
    date_from: date,
    date_to: date
) -> pd.DataFrame:
    return _call(
        entities=f"KRX:{code}",
        consolidated=consolidated,
        date_from=date_from,
        date_to=date_to
    )
