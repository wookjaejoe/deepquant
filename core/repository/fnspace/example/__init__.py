"""
Dart 공시 보고서 조회하고, FnSpace 에 재무정보 조회 API 호출하여 MongoDB에 수집한다.
"""

from core.dartx.search import search_reports
from core.repository import get_stocks
from core.dartx.search import get_fnym
from core.repository.fnspace.collect import collect_if_not_exist
from core.repository.fnspace import account
from base import log
import logging

log.init()

_logger = logging.getLogger()


def run(
    bgn_de: str,
    end_de: str,
):
    """
    :param bgn_de YYYYMMDD
    :param end_de YYYYMMDD
    """
    stocks = get_stocks()
    stock_count = len(stocks)
    num = 0
    for _, stock in stocks.iterrows():
        num += 1
        code = stock["stock_code"]
        name = stock["stock_name"]

        _logger.info(f"[{num}/{stock_count}] {name}")

        reports = search_reports(
            bgn_de=bgn_de,
            end_de=end_de,
            stock_code=stock["stock_code"],
        )

        for _, r in reports.iterrows():
            report_nm = r["report_nm"]
            _logger.info(f"Collecting for {report_nm}")
            ym = get_fnym(report_nm)

            for item in account.majors:
                collect_if_not_exist(
                    code=code,
                    year=ym.year,
                    month=ym.month,
                    item=item
                )
