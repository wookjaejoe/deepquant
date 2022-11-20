from base import log

log.init()

from repository import deepsearch
from repository.mongo import DsCollection
import time
import logging
from base import Quarter
from datetime import date


def update():
    titles = ["자본", "자산", "이익잉여금", "매출액", "매출총이익", "영업이익", "당기순이익", "영업활동으로인한현금흐름", "투자활동으로인한현금흐름"]
    query_list = []
    today = date.today()
    last_confirmed = Quarter.last_confirmed(today.year, today.month)
    for title in titles:
        for year in range(2000, today.year + 1):
            if year < last_confirmed.year:
                query_list += [(title, year, q) for q in range(1, 4 + 1)]
            elif year == last_confirmed.year:
                query_list += [(title, year, q) for q in range(1, last_confirmed.quarter + 1)]
            else:
                raise Exception("Something wrong.")

            if Quarter(year, 4) <= last_confirmed:
                # 4분기 실적이 나왔으면, 조회
                query_list.append((title, year))

    for query in query_list:
        title = query[0]
        year = query[1]
        quarter = None
        if len(query) == 2:
            pass
        elif len(query) == 3:
            quarter = query[2]
        else:
            raise Exception("Something wrong.")

        logging.info(query)

        if DsCollection.exists(title, year, quarter):
            logging.info(f"{query} already exists")
            continue

        try:
            content = deepsearch.query(title=title, year=year, quarter=quarter)
        except:
            time.sleep(1)
            content = deepsearch.query(title=title, year=year, quarter=quarter)

        DsCollection.insert_one(content, title=title, year=year, quarter=quarter)
        time.sleep(1)


update()
