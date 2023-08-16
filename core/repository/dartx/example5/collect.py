import json
import logging
import os
from datetime import date

import dart_fss as dart
from dart_fss.fs.extract import analyze_report
from pymongo import MongoClient

from base import log
from config import config
from core.repository.dartx import OpenDartApiKey

log.init()

_client = MongoClient(config["mongo"]["url"])
_collection = _client["finance"]["opendart"]
_logger = logging.getLogger()


def search():
    dart.set_api_key(OpenDartApiKey.next())
    page_no = 1
    while True:

        search_result = dart.search(
            bgn_de="20230701",
            end_de=date.today().strftime("%Y%m%d"),
            last_reprt_at="Y",
            pblntf_detail_ty="A002",
            page_no=page_no,
            page_count=100
        )

        for r in search_result:
            yield r

        if search_result.page_no >= search_result.total_page:
            break

        page_no += 1


def flat_index(multi_index):
    return multi_index.map(lambda tp: tp[0] + "/" + (";".join(tp[1]) if isinstance(tp[1], tuple) else tp[1]))


folder_name = "2023-2Q"
os.makedirs(folder_name, exist_ok=True)


def save(r, sep: bool):
    dart.set_api_key(OpenDartApiKey.next())
    filename = r.stock_code + ("_sep" if sep else "") + ".json"
    filepath = os.path.join(folder_name, filename)
    if os.path.isfile(filepath):
        _logger.info(f"{filepath} already exists.")
        return

    od = analyze_report(r, separate=sep)
    if od is None:
        _logger.info("Skip because od is None")
        return

    def preproc(df):
        if df is None:
            return None

        try:
            df.columns = flat_index(df.columns)
            return df.to_dict()
        except Exception as e:
            _logger.warning(str(e))
            return None

    doc = {k: preproc(v) for k, v in od.items()}

    with open(filepath, "w") as f:
        json.dump(doc, f)


def main():
    count = 1
    for r in search():
        _logger.info(f"[{count}] {r.corp_name}")
        if not r.stock_code:
            count += 1
            continue

        if "스팩" in r.corp_name:
            count += 1
            continue

        for sep in [True, False]:
            try:
                save(r, sep)
            except Exception as e:
                _logger.warning(str(e))

        count += 1
