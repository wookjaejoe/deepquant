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

from core import FsDb
from core.repository import get_stocks
from core.repository.maria.manage import *


def update_charts(fromdate: date):
    clear(fromdate)
    update_index_chart(fromdate)
    update_chart(fromdate)
    insert_month_chart(fromdate.year, fromdate.month)


def update_fs(fromyear: int):
    # 종목 정보
    stocks = get_stocks().set_index("stock_code")

    # 최신 종가
    chart = pd.read_sql(
        "select * from chart where date = (select max(date) from chart)",
        maria_home()
    ).set_index("code")

    df = stocks.join(chart)
    df = df[df["cap"].notna()].sort_values("cap", ascending=False)

    # 수집 시작
    fs_db = FsDb()
    fs_db.update_all(
        list(df.index),
        date_from=date(fromyear, 1, 1),
        date_to=date.today()
    )


def main():
    update_stocks()

    fromdate = date(2024, 3, 1)
    update_charts(fromdate)

    fromyear = 2023
    update_fs(fromyear)
    FsDb().make_table()


if __name__ == '__main__':
    main()
