from base import log

from repository import deepsearch as ds
from repository.mongo import DsCollection

log.init()

titles = ["자본", "자산", "매출액", "매출총이익", "영업이익", "당기순이익", "영업활동으로인한현금흐름", "투자활동으로인한현금흐름"]


def main():
    year = 2022
    quarter = 3
    update_items = [(title, year, quarter) for title in titles]

    for update_item in update_items:
        # fetch
        content = ds.query(*update_item)
        # insert
        DsCollection.insert_one(
            content,
            title=update_item[0],
            year=update_item[1],
            quarter=update_item[2]
        )


if __name__ == '__main__':
    main()
