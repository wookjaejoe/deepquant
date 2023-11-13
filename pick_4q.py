import pandas as pd

from core.dartx.fnlttSinglAcntAll import request_report

# x = search_reports(
#     bgn_de="20230930",
# )

report = request_report(
    corp_code="00409964",
    bsns_year=2023,
    reprt_code="11014",
    fs_div="OFS"
)
report = pd.DataFrame(report["list"])

print()

# rcept_no 20231113000241
# corp_code 00409964
# stock_code 106190
