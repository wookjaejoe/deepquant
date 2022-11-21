from dataclasses import dataclass, fields
from datetime import date
from typing import *

import pandas as pd

from base.time import YearMonth
from .conn import MariaConnection, maria_home
from base.cache import cache_file as cache_file
import sqlite3

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

    result = pd.DataFrame(rows, columns=chart_fields)
    result.index = result['code']
    result = result.drop('code', axis=1)
    return result


def upload_month_chart():
    def _query(year: int, month: int):
        return f"""
        SELECT code,
           MAX(date)                                                                 as date,
           cast(SUBSTRING_INDEX(MIN(CONCAT(date, '_', open)), '_', -1) as unsigned)  as open,
           cast(MAX(high) as unsigned)                                               as high,
           cast(MIN(low) as unsigned)                                                as low,
           cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', close)), '_', -1) as unsigned) as close,
           cast(SUM(vol) as unsigned)                                                as vol,
           cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', cap)), '_', -1) as unsigned)   AS cap
        FROM historical_chart
        where year(date) = {year} and month(date) = {month}
        group by code, year(date), month(date)
        order by date;
        """

    yms = set([YearMonth.of(d) for d in get_bussness_dates(date(1996, 1, 1), date(2022, 10, 31))])
    engine = maria_home()
    result = pd.DataFrame()
    for ym in min(yms).iter(max(yms)):
        print(ym)
        df = pd.read_sql(_query(ym.year, ym.month), maria_home())
        result = pd.concat([result, df])

    result.to_sql("_month_chart", engine, index=False)
