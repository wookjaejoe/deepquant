"""
collect - dart 에서 "매출액 또는 손익구조 변동" 수시 공시 모두 검색하고 다운로드
load - 수집한 모든 리포트 zip 파일 로드
calc_qoq - 분기 정보 계산
"""

import os
import re
import zipfile
from functools import reduce

import dart_fss as dart
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from core.dartx.apikey import OpenDartApiKey
from core.fs import FsLoader, AccAlias, Growth
from utils import pdutil
from utils.numeric import to_numeric


def collect(report_year: int):
    """
    :param report_year: 리포트 발표 년도
    """
    target_folder = f"{report_year}년도_매출액또는손익구조변동"
    os.makedirs(target_folder, exist_ok=True)

    page_no = 1
    while True:
        dart.set_api_key(OpenDartApiKey.next())

        print(page_no)
        search_results = dart.search(
            bgn_de=f'{report_year}0101',
            last_reprt_at="Y",
            pblntf_detail_ty='I001',
            page_no=page_no,
            page_count=100
        )

        reports = [r for r in search_results.report_list if "매출액또는손익구조" in r.report_nm]
        for r in reports:
            for f in r.attached_files:
                if not r.stock_code:
                    continue

                f.filename = f"{r.stock_code}_{f.filename}"
                print(f.filename)
                f.download(target_folder)

        page_no += 1
        if page_no > search_results.total_page:
            break


def load(report_year: int):
    """
    :param report_year: 리포트 발표 년도
    """
    result = pd.DataFrame()
    target_folder = f"{report_year}년도_매출액또는손익구조변동"
    for filename in os.listdir(target_folder):
        if not filename.endswith(".zip"):
            continue

        st_code = filename[:6]
        with zipfile.ZipFile(os.path.join(target_folder, filename)) as zip_ref:
            for inner_name in zip_ref.namelist():
                print(inner_name)
                inner_file = zip_ref.open(inner_name)
                rpt = _parse_html(inner_file).to_frame().T
                rpt["code"] = st_code
                rpt["date"] = re.findall(r'\(\d{4}\.\d{2}\.\d{2}\)', inner_name)[-1][1:-1]
                result = pd.concat([result, rpt])

    result = (
        result
        .sort_values("date")
        .drop_duplicates("code", keep="last")
        .set_index("code")
    )

    result.to_csv("매출액또는손익구조변경.csv")
    return result


def _parse_html(content) -> pd.Series:
    # html 파싱
    soup = BeautifulSoup(content, 'html.parser')
    tables = soup.find_all("table")

    # 매출액또는손익구조변동내용 테이블 탐색
    table = None
    for t in tables:
        text = t.text.replace(" ", "")
        if "재무제표의종류" in text and "매출액또는손익구조변동내용" in text:
            table = t
            break

    # 공백 제거
    rows = []
    for tr in table.find_all("tr"):
        rows.append([td.text.replace(" ", "") for td in tr.find_all("td")])

    # 손익계산서 단위, 재무상태표 단위
    is_unit, fs_unit = 1, 1
    result = pd.Series()

    def put(title, curr, prev, unit: int):
        """Dataframe 에 당해년도, 직전연도 값 추가"""
        try:
            result[f"{title}/Y"] = to_numeric(curr) * unit
        except:
            result[f"{title}/Y"] = np.nan

        try:
            result[f"{title}/Y-1"] = to_numeric(prev) * unit
        except:
            result[f"{title}/Y-1"] = np.nan

    def parse_unit(s: str):
        """단위 파싱"""
        if "단위:원" in s:
            return 1
        elif "단위:천원" in s:
            return 1000
        elif "USD" in s:
            return np.nan
        else:
            raise RuntimeError(f"Unable to parse unit in {s}")

    for row in rows:
        head = row[0]
        if "재무제표의종류" in head:
            result["종류"] = row[1]
        elif "매출액또는손익구조변동내용" in head:
            if len(row) < 3:
                # 주석 형식으로 잔뜩 써놓은 셀이 있는데, 내가 원하는 행이 아님.
                continue

            assert row[1], row[2] == ("당해사업연도", "직전사업연도")
            is_unit = parse_unit(head)
        elif "재무현황" in head:
            if len(row) < 3:
                # 주석 형식으로 잔뜩 써놓은 셀이 있는데, 내가 원하는 행이 아님.
                continue

            assert row[1], row[2] == ("당해사업연도", "직전사업연도")
            fs_unit = parse_unit(head)
        elif head.endswith("매출액"):
            put("R", row[1], row[2], is_unit)
        elif head.endswith("영업이익"):
            put("O", row[1], row[2], is_unit)
        elif head.endswith("법인세비용차감전계속사업이익"):
            put("EBT", row[1], row[2], is_unit)
        elif head.endswith("당기순이익"):
            put("E", row[1], row[2], is_unit)
        elif head.endswith("자산총계"):
            put("EQ", row[1], row[2], fs_unit)
        elif head.endswith("부채총계"):
            put("D", row[1], row[2], fs_unit)
        elif head.endswith("자본총계"):
            put("A", row[1], row[2], fs_unit)

    return result


# noinspection DuplicatedCode
def calc(report_year: int):
    year = report_year - 1
    fs_loader = FsLoader.instance()
    df = pd.read_csv("매출액또는손익구조변경.csv", dtype={"code": str})
    df["consolidated"] = df["종류"].replace({"개별": 0, "연결": 1})
    consolidated = df[["code", "consolidated"]].set_index("code")["consolidated"]

    def get_consolidated(x: pd.DataFrame):
        code = x.iloc[0]["code"]
        return consolidated[code] if code in consolidated else -1

    indexer = df.groupby("code").apply(get_consolidated).to_frame("consolidated").reset_index()
    indexer = pd.MultiIndex.from_frame(indexer)

    fins = [pdutil.find(fs_loader.table, year=year, qtr=q) for q in [1, 2, 3]]
    fins = [fin.set_index(["code", "consolidated"]) for fin in fins]
    fins = [fin[fin.index.isin(indexer)].reset_index(level=1) for fin in fins]
    codes = reduce(set.intersection, map(lambda x: set(x), [f.index for f in fins]))
    fins = [fin.loc[list(codes)] for fin in fins]
    is_cols = ["영업이익", "법인세비용차감전계속영업이익", "당기순이익"]
    pre = fs_loader.load(year - 1, 4, get_consolidated)
    df = df.set_index("code")
    result = pd.DataFrame(pre.index).set_index("code")
    for is_col in is_cols:
        acc = AccAlias[is_col]
        aft = df[f"{acc}/Y"] - pd.concat([fin[is_col] for fin in fins], axis=1).sum(axis=1)
        result[f"{acc}/{year - 1}-4Q"] = pre[acc]
        result[f"{acc}/{year}-4Q"] = aft
        result[f"{acc}_QoQ"] = Growth.rate(aft=aft, pre=pre[acc]).replace({np.nan: 0})
        result[f"{acc}/EQ_QoQ"] = aft / df["EQ/Y"] - pre[acc] / df["EQ/Y"]

        result[f"{acc}_QoQ_pct"] = np.ceil(result[f"{acc}_QoQ"].rank(pct=True) * 100)
        result[f"{acc}/EQ_QoQ_pct"] = np.ceil(result[f"{acc}/EQ_QoQ"].rank(pct=True) * 100)

    result.to_csv("매출액또는손익구조변경2.csv")
    return result
