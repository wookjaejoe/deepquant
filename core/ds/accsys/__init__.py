import pandas as pd
import os
from typing import *

# 엑셀 파일 경로 설정
_excel_file_path = os.path.join(os.path.dirname(__file__), "DeepSearch_재무계정체계_220706.xlsx")


def extract() -> Dict[str, pd.DataFrame]:
    """
    딥서치에서 제공하는 재무계정체계 엑셀파일에서 시트를 모두 읽어 반환한다.
    """
    # 엑셀 파일 읽기
    xls = pd.ExcelFile(_excel_file_path, engine="openpyxl")

    # 각 시트를 DataFrame으로 변환
    sheet_names = xls.sheet_names
    dataframes = {sheet_name: xls.parse(sheet_name) for sheet_name in sheet_names}
    return dataframes


def extract_ifrs():
    acc_all = extract()

    result = acc_all["IFRS연결"]
    result["consolidated"] = True

    ifrs_sep = acc_all["IFRS별도"]
    ifrs_sep["consolidated"] = False

    return pd.concat([result, ifrs_sep])
