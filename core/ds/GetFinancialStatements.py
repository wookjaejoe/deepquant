# https://help.deepsearch.com/dp/api/func/company/financial/getfinancialstatements

import base64
import logging
from datetime import date
from typing import *

import pandas as pd
import requests
from retry import retry
from config import config

_logger = logging.getLogger()


@retry(tries=5, delay=1, jitter=1)
def _call_api(
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
    _input = f"""
    GetFinancialStatements(
        {entities},
        report_type="{report_type}",
        consolidated={consolidated},
        is_annual={is_annual},
        is_accumulated={is_accumulated},
        report_ids={report_ids},
        date_from={date_from},
        date_to={date_to}
    )
    """
    _input = base64.b64encode(_input.encode("utf8")).decode("utf8")
    response = requests.post(
        "https://www.deepsearch.com/api/app/v1/compute",
        data="{\"input\":" + f"\"{_input}\"" + "}",
        headers={
            "authorization": config["deepSearchAuth"],
            "content-type": "application/json",
            "x-deepsearch-encoded-input": "true",
        }
    )
    assert response.status_code == 200, f"Status code: {response.status_code}"
    assert len(response.json()["data"]["exceptions"]) == 0
    return response


def call_api(
    code: str,
    consolidated: bool,
    date_from: date,
    date_to: date
) -> pd.DataFrame:
    # noinspection PyCallingNonCallable
    res = _call_api(
        entities=f"KRX:{code}",
        consolidated=consolidated,
        date_from=date_from,
        date_to=date_to
    )
    content = res.json()
    return pd.DataFrame(content["data"]["pods"][1]["content"]["data"])
