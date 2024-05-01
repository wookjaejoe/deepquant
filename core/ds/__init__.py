import base64
import json

import pandas as pd
import requests

from config import config
from core.ds.exceptions import AuthenticationError


def call(func_name: str, *args, **kwargs):
    params = ",".join([
        s for s in [
            ",".join(args),
            ",".join([f"{k}={v}" for k, v in kwargs.items()])
        ] if s
    ])
    query = f"""{func_name}({params})"""

    res = requests.post(
        "https://www.deepsearch.com/api/app/v1/compute",
        data=json.dumps({"input": base64.b64encode(query.encode("utf8")).decode("utf8")}),
        headers={
            "authorization": config["deepSearchAuth"],
            "content-type": "application/json",
            "x-deepsearch-encoded-input": "true",
        }
    )

    if res.status_code == 426:
        raise AuthenticationError()

    assert res.status_code == 200, f"Status code: {res.status_code}"
    assert len(res.json()["data"]["exceptions"]) == 0, res.json()["data"]["exceptions"]

    content = res.json()
    return pd.DataFrame(pd.DataFrame(content["data"]["pods"][1]["content"]["data"]))
