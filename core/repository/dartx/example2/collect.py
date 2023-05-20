import os.path

import dart_fss as dart
import pandas as pd

from core.repository.maria.conn import maria_home
import pickle

dart.set_api_key("3835de6f6564a072832cc4ed390fbcdf6a490152")

stocks = pd.read_sql("select * from stock", maria_home())
codes = list(stocks["code"])


def find_quarter_report_and_save(corp):
    try:
        reports = corp.search_filings(
            bgn_de="20230101",
            last_reprt_at="Y",
            pblntf_detail_ty="A003"
        )

        assert reports.total_count > 0

        xbrl = reports[0].xbrl
        consolidated = xbrl.exist_consolidated()

        # 연결재무제표 존재 여부 확인( True / False)
        if consolidated:
            fin = xbrl.get_financial_statement()[0].to_DataFrame()
            inc = xbrl.get_income_statement()[0].to_DataFrame()
        else:
            fin = xbrl.get_financial_statement(separate=True)[0].to_DataFrame()
            inc = xbrl.get_income_statement(separate=True)[0].to_DataFrame()

        result = {
            "fin": fin,
            "inc": inc,
            "consolidated": consolidated
        }

        with open(f"2023-1Q/{corp.stock_code}.pickle", "wb") as fw:
            pickle.dump(result, fw)
    except Exception as e:
        print(f"[WARN] {str(e)}")


def collect_quarter_report_pickle():
    corp_list = dart.get_corp_list()
    corp_list = [corp for corp in corp_list if corp.stock_code in codes]
    for i in range(len(corp_list)):
        corp = corp_list[i]
        print(f"[{i + 1}/{len(corp_list)}]", corp.corp_name, corp.stock_code)
        if os.path.isfile(f"2023-1Q/{corp.stock_code}.pickle"):
            continue

        find_quarter_report_and_save(corp)
