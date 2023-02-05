from dataclasses import dataclass, fields
from datetime import date
from typing import *

import pandas as pd

from base.timeutil import YearMonth
from .conn import MariaConnection, maria_home

TABLE_HISTORICAL_CHART = "historical_chart"


@dataclass
class ChartSnapshot:
    code: str
    date: date
    open: int
    high: int
    low: int
    close: int
    vol: int
    cap: int


def get_dates_in_chart(table_name: str, start: date, end: date) -> Iterator[date]:
    with MariaConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            select distinct date from {table_name}
            where date >= '{start}' and date <= '{end}'
            """
        )

        for row in cursor:
            yield row[0]

        cursor.close()


def get_bussness_dates(start: date, end: date) -> Iterator[date]:
    return get_dates_in_chart(TABLE_HISTORICAL_CHART, start, end)


def get_day_chart(d: date) -> pd.DataFrame:
    chart_fields = [field.name for field in fields(ChartSnapshot)]
    with MariaConnection() as conn:
        cursor = conn.cursor()
        query = f"""
        select {", ".join(chart_fields)} from historical_chart
        where date='{d}'
        """
        cursor.execute(query)
        rows = list(cursor.fetchall())
        cursor.close()

    return pd.DataFrame(rows, columns=chart_fields).set_index("code")


def get_month_chart(year: int, month: int) -> pd.DataFrame:
    print(f"select * from month_chart where year(date) = {year} and month(date) = {month};")
    return pd.read_sql(
        sql=f"select * from month_chart where year(date) = {year} and month(date) = {month};",
        con=maria_home()
    )


def _query_month_chart(year: int, month: int):
    return f"""
    SELECT code,
       MAX(date)                                                                 as date,
       cast(SUBSTRING_INDEX(MIN(CONCAT(date, '_', open)), '_', -1) as unsigned)  as open,
       cast(MAX(high) as unsigned)                                               as high,
       cast(MIN(low) as unsigned)                                                as low,
       cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', close)), '_', -1) as unsigned) as close,
       cast(SUM(vol) as unsigned)                                                as vol,
       cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', vol)), '_', -1) as unsigned)   as vol_lastday,
       cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', cap)), '_', -1) as unsigned)   as cap
    FROM historical_chart
    where year(date) = {year} and month(date) = {month}
    group by code, year(date), month(date)
    order by date;
    """


def upload_month_chart():
    dates = pd.read_sql("select distinct date from historical_chart", maria_home())["date"]
    yms = sorted({YearMonth.from_date(d) for d in dates})
    assert len(yms) == len(set(yms))
    result = pd.DataFrame()
    min(yms)
    for ym in min(yms).to(max(yms)):
        print(ym)
        result = pd.concat([result, pd.read_sql(_query_month_chart(ym.year, ym.month), maria_home())])

    result.to_sql("month_chart", maria_home(), index=False)
