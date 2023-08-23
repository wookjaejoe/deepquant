"""
OpenDart API > XBRL 이용한 재무데이터 수집
"""

import dart_fss as dart
import pandas as pd
from datetime import date
from retry import retry
from base import log
import logging
from pymongo import MongoClient
from config import config
import os

from core.repository.maria.conn import maria_home
from core.repository.dartx import OpenDartApiKey

log.init()
_logger = logging.getLogger()
_client = MongoClient(config["mongo"]["url"])
_collection = _client["finance"]["opendart"]

stocks = pd.read_sql("select * from stock", maria_home())


def search_reports(corp):
    page_no = 1
    while True:
        dart.set_api_key(OpenDartApiKey.next())
        search_result = corp.search_filings(
            bgn_de="20000101",
            end_de=date.today().strftime('%Y%m%d'),
            last_reprt_at="Y",
            pblntf_ty="A",
            # pblntf_detail_ty="A003",
            pblntf_detail_ty="A002",
            page_no=page_no
        )

        for r in search_result:
            yield r

        if search_result.page_no >= search_result.total_page:
            break

        page_no += 1


def xbrl_to_doc(xbrl) -> dict:
    exist_consolidated = xbrl.exist_consolidated()
    separate = not exist_consolidated

    def preproc(tables):
        return [t.to_DataFrame().to_json() for t in tables]

    return {
        "exist_consolidated": exist_consolidated,
        "financial_statement": preproc(xbrl.get_financial_statement(separate=separate)),
        "income_statement": preproc(xbrl.get_income_statement(separate=separate)),
        "changes_in_equity": preproc(xbrl.get_changes_in_equity(separate=separate)),
        "cash_flows": preproc(xbrl.get_cash_flows(separate=separate)),
    }


@retry(exceptions=OverflowError, tries=10, delay=1)
def collect_report(r):
    dart.set_api_key(OpenDartApiKey.next())
    doc = {
        "_id": r.rcept_no,
        "corp_code": r.corp_code,
        "corp_name": r.corp_name,
        "stock_code": r.stock_code,
        "report_nm": r.report_nm,
        "rcept_dt": r.rcept_dt
    }

    try:
        if not r.xbrl:
            raise Exception("No xbrl")

        doc.update({"xbrl": xbrl_to_doc(r.xbrl)})
        print(r.report_nm, "OK")
    except Exception as e:
        doc.update({"xbrl_error": str(e)})
        print(r.report_nm, str(e))
    finally:
        _collection.insert_one(doc)


def collect_corp_reports(corp):
    _logger.info(str(corp))
    reports = search_reports(corp)
    dart.set_api_key(OpenDartApiKey.next())
    for r in reports:
        collect_report(r)


from dart_fss.errors import OverQueryLimit, NoDataReceived


def extract(corp, separate):
    try:
        filename = corp.stock_code + ("_sep.xlsx" if separate else ".xlsx")
        filepath = os.path.join("fsdata", filename)
        if os.path.isfile(filepath):
            _logger.info(f"{filepath} already exists.")
            return

        dart.extract(
            corp.corp_code,
            bgn_de="20130101",
            report_tp=["quarter"],
            separate=separate
        ).save(filename=filename)
    except NoDataReceived:
        pass
    except OverQueryLimit:
        raise
    except Exception as e:
        _logger.error(f"Fail about {corp}", exc_info=e)


def main():
    dart.set_api_key(OpenDartApiKey.next())
    corp_list = [c for c in dart.get_corp_list() if c.stock_code and "스팩" not in c.corp_name]
    i = 0
    for corp in corp_list:
        _logger.info(f"[{i}/{len(corp_list)}] {corp}")
        extract(corp, separate=True)
        extract(corp, separate=False)
        i += 1
