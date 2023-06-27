import time

from base import log
from base.timeutil import YearQuarter
from core.repository.deepsearch import query as dsquery
from core.repository.mongo import DsCollection

log.init()

titles = [
    "자본총계", "자산총계", "부채총계",
    "매출액", "매출총이익", "영업이익", "당기순이익"
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


def collect(year: int, quarter: int):
    for title in titles:
        try:
            fetch_and_update(title, year, quarter)
        except:
            print(f"[WARN] The 'fetch_and_update' failed once.")
            time.sleep(5)
            fetch_and_update(title, year, quarter)


def collect_many(start: YearQuarter, end: YearQuarter):
    query_list = []
    for title in titles:
        for q in start.to(end):
            query_list.append((title, q))

    count = 0
    for query_item in query_list:
        count += 1
        title = query_item[0]
        quarter = query_item[1]
        print(f"[{count}/{len(query_list)}]", title, quarter)
        collect(year=quarter.year, quarter=quarter.quarter)
