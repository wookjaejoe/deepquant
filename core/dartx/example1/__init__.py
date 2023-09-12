import os
import zipfile
from typing import *

# noinspection PyPackageRequirements
import Levenshtein
import dart_fss as dart
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from retry import retry
import warnings

import core.repository.deepsearch.loader as ds
from core.repository.maria.conn import maria_home

dart.set_api_key("3835de6f6564a072832cc4ed390fbcdf6a490152")

# 모든 상장된 기업 리스트 불러오기

maria_db = maria_home()
target_folder = "2022년도매출액또는손익구조변동"


@retry(tries=3, delay=10)
def load_매출액또는손익구조(st_code: str, co_code: str):
    # 거래소 공시 조회
    try:
        reports = dart.search(corp_code=co_code, bgn_de='20230101', pblntf_detail_ty='I001')
    except Exception as e:
        print(str(e))
        return

    reports = [r for r in reports.report_list if "매출액또는손익구조" in r.report_nm]
    if not reports:
        return

    files = {}
    for report in reports:
        for file in report.attached_files:
            files.update({file.rcp_no: file})

    file = files[sorted(files)[-1]]
    file.filename = f"{st_code}_{file.filename}"
    file.download(target_folder)


def check_similarity(s1: str, s2: str):
    s1 = s1.replace(" ", "").replace("-", "")
    s2 = s2.replace(" ", "").replace("-", "")
    return Levenshtein.ratio(s1, s2) > 0.9


def find_title(s: str, in_titles: List[str]):
    for title in in_titles:
        if check_similarity(s, title):
            return title

    return None


def coname_from_filename(filename: str):
    start_idx = filename.find('[') + 1
    end_idx = filename.find(']')
    return filename[start_idx:end_idx]


def st_code_from_filename(filename: str):
    return filename[:6]


def is_modified(filename: str):
    return "[정정]" in filename


def parse_percent(s: str):
    s = s.replace(",", "").replace(" ", "").replace("%", "").replace("(", "").replace(")", "")
    if s == "적자전환":
        return -100
    if s == "흑자전환":
        return 100

    try:
        return float(s)
    except:
        return np.nan


def find_target_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        text = table.text.replace(" ", "")
        if "1.재무제표의종류" in text and "2.매출액또는손익구조변동내용" in text:
            return table


rows = []


def read_report_zip(file_path):
    st_code = st_code_from_filename(os.path.basename(file_path))
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # zip 파일 내 html 파일에 대해 반복문 실행
        for inner_file_name in zip_ref.namelist():
            if not inner_file_name.endswith('.html'):
                continue

            inner_file = zip_ref.open(inner_file_name)
            coname = coname_from_filename(inner_file_name)
            modified = is_modified(inner_file_name)
            soup = BeautifulSoup(inner_file, 'html.parser')

            # html 파일 내 테이블 형식 데이터 파싱
            table = find_target_table(soup)
            if not table:
                print(f"Cannot found table in file: {inner_file_name}")
                return

            trs = table.find_all("tr")
            unit1, unit2 = 1, 1
            check1, check2 = False, False
            for i in range(len(trs)):
                tr = trs[i]
                tds = tr.find_all("td")
                head = tds[0].text.replace(" ", "")
                if not check1 and head.startswith("2.매출액또는손익구조변동내용"):
                    check_similarity(tds[1].text, "당해사업연도")
                    check_similarity(tds[2].text, "직전사업연도")
                    check1 = True
                    if "천원" in head:
                        unit1 = 1000
                elif not check2 and head.startswith("3.재무현황"):
                    check_similarity(tds[1].text, "당해사업연도")
                    check_similarity(tds[2].text, "직전사업연도")
                    check2 = True
                    if "천원" in head:
                        unit2 = 1000

            assert check1 and check2

            row = {
                "code": st_code,
                "coname": coname,
                "정정": modified,
            }

            for tr in trs:
                td_list = tr.find_all("td")
                title = find_title(td_list[0].text, ["매출액", "영업이익", "법인세비용차감전계속사업이익", "당기순이익"])
                if title:
                    row.update(
                        {
                            f"당해년도_{title}": parse_percent(td_list[1].text) * unit1,
                            f"직전년도_{title}": parse_percent(td_list[2].text) * unit1,
                        }
                    )
                    continue

                title = find_title(td_list[0].text, ["자산총계", "자본총계"])
                if title:
                    row.update(
                        {
                            f"당해년도_{title}": parse_percent(td_list[1].text) * unit2,
                            f"직전년도_{title}": parse_percent(td_list[2].text) * unit2,
                        }
                    )
                    continue

        rows.append(row)


def read_all():
    filenames = list(os.listdir(target_folder))
    i = 0
    for filename in filenames:
        i += 1
        print(f"[{i}/{len(filenames)}] {filename}")
        if not filename.endswith('.zip'):
            continue

        file_path = os.path.join(target_folder, filename)
        read_report_zip(file_path)

    result = pd.DataFrame(rows)
    result.to_csv("매출액또는손익구조30%(대규모법인은15%)이상변경.csv", index=False)
    return result


def load_all():
    i = 0
    co_list = dart.get_corp_list()
    stocks = pd.read_sql_table("stock", maria_db)
    total = len(stocks)
    for st_code in stocks["code"]:
        i += 1
        print(f"[{i}/{total}] {st_code}")
        co = co_list.find_by_stock_code(st_code, True, True)
        if not co:
            print(f"Not found. code={st_code}")
            continue

        co_code = co.corp_code

        try:
            load_매출액또는손익구조(st_code, co_code)
        except Exception as e:
            warnings.warn(f"Failed to load {st_code}:{co_code}.")


def put_together(df: pd.DataFrame):
    df.set_index("code", inplace=True)

    # 2022년 4분기 매출액: 당해년도_매출액 - sum(2022-1,2,3분기매출액)
    df["분기매출액"] = df["당해년도_매출액"] - sum([
        ds.load_by_quarter("매출액", 2022, 1),
        ds.load_by_quarter("매출액", 2022, 2),
        ds.load_by_quarter("매출액", 2022, 3)
    ])

    df["분기영업이익"] = df["당해년도_영업이익"] - sum([
        ds.load_by_quarter("영업이익", 2022, 1),
        ds.load_by_quarter("영업이익", 2022, 2),
        ds.load_by_quarter("영업이익", 2022, 3)
    ])

    df["분기순이익"] = df["당해년도_당기순이익"] - sum([
        ds.load_by_quarter("당기순이익", 2022, 1),
        ds.load_by_quarter("당기순이익", 2022, 2),
        ds.load_by_quarter("당기순이익", 2022, 3)
    ])

    def qoq(title: str):
        x = ds.load_by_quarter(title, 2021, 4)
        y = df[f"분기{title}"]
        return (y - x) / x

    def qoq_with_asset(title: str):
        return (df[f"분기{title}"] / df["당해년도_자산총계"]) - (ds.load_by_quarter(title, 2021, 4) / df["직전년도_자산총계"])

    df["매출액_QoQ"] = qoq("매출액")
    df["영업이익_QoQ"] = qoq("영업이익")

    df["매출액/자산총계_QoQ"] = qoq_with_asset("매출액")
    df["영업이익/자산총계_QoQ"] = qoq_with_asset("영업이익")

    df["매출액/자산총계_QoQ_pct"] = df["매출액/자산총계_QoQ"].rank(pct=True)
    df["영업이익/자산총계_QoQ_pct"] = df["영업이익/자산총계_QoQ"].rank(pct=True)
    df.to_csv("2022-4Q.csv")


def run():
    """
    20230101 이후에 공시된 매출액 또는 손익 구조 변경 리포트를 다운로드한 후 종합하여 csv 파일을 생성한다.
    """
    # load_all()
    df = read_all()
    put_together(df)
