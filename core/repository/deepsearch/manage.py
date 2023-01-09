import time

from base import log
from base.timeutil import YearQuarter
from core.repository.deepsearch import query as dsquery
from core.repository.mongo import DsCollection

log.init()

titles = [
    "자본총계", "자산총계", "부채총계", "유동자산", "비유동자산"
    "매출액", "매출총이익", "영업이익", "당기순이익",
    "영업활동으로인한현금흐름",
]


def fetch_and_update(title: str, year: int, quarter: int):
    # fetch
    content = dsquery.query(title, year, quarter)
    # insert
    DsCollection.insert_one(
        content,
        title=title,
        year=year,
        quarter=quarter
    )


def update():
    query_list = []
    for title in titles:
        for q in YearQuarter(2000, 1).to(YearQuarter(2022, 3)):
            query_list.append((title, q))

    count = 0
    for query_item in query_list:
        count += 1
        title = query_item[0]
        quarter = query_item[1]
        print(f"[{count}/{len(query_list)}]", title, quarter)

        try:
            fetch_and_update(title, quarter.year, quarter.quarter)
        except:
            time.sleep(5)
            fetch_and_update(title, quarter.year, quarter.quarter)
