import base64
from datetime import date

import requests
from retry import retry

from config import config


@retry(tries=3, delay=1, jitter=3)
def call_api(code: str) -> dict:
    assert len(code) == 6
    _input = f"GetAvailableFinancialStatements(KRX:{code}, date_from=1990-01-01, date_to={date.today()})"
    _input = base64.b64encode(_input.encode("utf8")).decode("utf8")
    res = requests.get(
        "https://www.deepsearch.com/api/app/v1/compute",
        data="{\"input\":" + f"\"{_input}\"" + "}",
        headers={
            "authorization": config["deepSearchAuth"],
            "content-type": "application/json",
            "x-deepsearch-encoded-input": "true",
        }
    )
    assert res.status_code == 200, f"Status code: {res.status_code}"
    return res.json()
