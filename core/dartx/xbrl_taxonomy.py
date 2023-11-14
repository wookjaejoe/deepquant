"""
재무제표구분	재무제표명칭	    개별/연결	표시방법	    세전세후
BS1	        재무상태표	        연결	    유동/비유동법
BS2	        재무상태표	        개별	    유동/비유동법
BS3	        재무상태표	        연결	    유동성배열법
BS4	        재무상태표	        개별	    유동성배열법
IS1	        별개의 손익계산서	연결	    기능별분류
IS2	        별개의 손익계산서	개별	    기능별분류
IS3	        별개의 손익계산서	연결	    성격별분류
IS4	        별개의 손익계산서	개별	    성격별분류
CIS1	    포괄손익계산서	    연결	                세후
CIS2	    포괄손익계산서	    개별	                세후
CIS3	    포괄손익계산서	    연결	                세전
CIS4	    포괄손익계산서	    개별	                세전
DCIS1	    단일 포괄손익계산서	연결	    기능별분류	    세후포괄손익
DCIS2	    단일 포괄손익계산서	개별	    기능별분류	    세후포괄손익
DCIS3	    단일 포괄손익계산서	연결	    기능별분류	    세전
DCIS4	    단일 포괄손익계산서	개별	    기능별분류	    세전
DCIS5	    단일 포괄손익계산서	연결	    성격별분류	    세후포괄손익
DCIS6	    단일 포괄손익계산서	개별	    성격별분류	    세후포괄손익
DCIS7	    단일 포괄손익계산서	연결	    성격별분류	    세전
DCIS8	    단일 포괄손익계산서	개별	    성격별분류	    세전
CF1	        현금흐름표	        연결	    직접법
CF2	        현금흐름표	        개별	    직접법
CF3	        현금흐름표	        연결	    간접법
CF4	        현금흐름표	        개별	    간접법
SCE1	    자본변동표	        연결
SCE2	    자본변동표	        개별
"""
import requests

from core.dartx import OpenDartApiKey
import pandas as pd
from core.repository import maria_home

cats = [
    "BS1",
    "BS2",
    "BS3",
    "BS4",
    "IS1",
    "IS2",
    "IS3",
    "IS4",
    "CIS1",
    "CIS2",
    "CIS3",
    "CIS4",
    "DCIS1",
    "DCIS2",
    "DCIS3",
    "DCIS4",
    "DCIS5",
    "DCIS6",
    "DCIS7",
    "DCIS8",
    "CF1",
    "CF2",
    "CF3",
    "CF4",
    "SCE1",
    "SCE2",
]


def fetch_all():
    db = maria_home()
    result = pd.DataFrame()

    for cat in cats:
        print(cat)
        res = requests.get(
            "https://opendart.fss.or.kr/api/xbrlTaxonomy.json",
            params={
                "crtfc_key": OpenDartApiKey.next(),
                "sj_div": cat
            }
        )
        df = pd.DataFrame(res.json()["list"])
        result = pd.concat([result, df])

    result.to_sql("xbrlTaxonomy", db, if_exists="replace", index=False)


