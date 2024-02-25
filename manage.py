"""
한 달에 1회 정도 아래 데이터 업데이트

0. Index chart
update_index_chart(date(2024, 1, 1)) 통해 지수 데이터 수집

0. Daily stock prices
update_chart(date(2024, 1, 1)) 통해 일별 주가 데이터 수집

0. Monthly stock prices
insert_month_chart(2024, 1)

0. Financial data
"""

from datetime import date
from core.repository.maria.manage import *


def main():
    fromdate = date(2024, 2, 1)

    # update_stocks()
    clear(fromdate)
    update_index_chart(fromdate)
    update_chart(fromdate)
    insert_month_chart(fromdate.year, fromdate.month)


if __name__ == '__main__':
    main()
