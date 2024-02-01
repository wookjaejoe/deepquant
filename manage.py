"""
매월 말일 또는 다음달 초 아래 데이터 수집
0. Index chart
update_index_chart(date(2024, 1, 1)) 통해 지수 데이터 수집

0. Daily stock prices
update_chart(date(2024, 1, 1)) 통해 일별 주가 데이터 수집

0. Monthly stock prices
insert_month_chart(2024, 1)

0. Financial data


"""

from datetime import date
from core.repository.maria.manage import update_chart, update_index_chart, insert_month_chart


def main():
    update_index_chart(date(2024, 1, 1))
    update_chart(date(2024, 1, 1))
    insert_month_chart(2024, 1)


if __name__ == '__main__':
    main()
