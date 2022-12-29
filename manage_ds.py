import time

from base import log

from repository import deepsearch as ds
from repository.mongo import DsCollection
from base.time import Quarter

log.init()

titles = [
    "자본총계", "자산총계", "부채총계",
    "매출액", "매출총이익", "영업이익", "당기순이익",
    "영업활동으로인한현금흐름",
]


def fetch_and_update(title: str, year: int, quarter: int):
    # fetch
    content = ds.query(title, year, quarter)
    # insert
    DsCollection.insert_one(
        content,
        title=title,
        year=year,
        quarter=quarter
    )


def main():
    query_list = []
    for title in titles:
        for q in Quarter(2000, 1).to(Quarter(2022, 3)):
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


if __name__ == '__main__':
    main()
