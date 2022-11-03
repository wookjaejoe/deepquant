from .conn import MariaConnection
from typing import *
from dataclasses import dataclass, fields
from datetime import date
import pandas


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


def get_bussness_months(start: date, end: date) -> Iterator[date]:
    return get_dates_in_chart("month_chart", start, end)


def get_bussness_dates(start: date, end: date) -> Iterator[date]:
    return get_dates_in_chart("historical_chart", start, end)


def get_day_chart(d: date) -> pandas.DataFrame:
    chart_fields = [field.name for field in fields(ChartSnapshot)]
    with MariaConnection() as conn:
        cursor = conn.cursor()
        query = f"""
        select {", ".join(chart_fields)} from historical_chart
        where date='{d}'
        """
        cursor.execute(query)

    rows = list(cursor.fetchall())
    result = pandas.DataFrame(rows, columns=chart_fields)
    result.index = result['code']
    result = result.drop('code', axis=1)
    return result


def get_month_chart(year: int, month: int) -> pandas.DataFrame:
    chart_fields = [field.name for field in fields(ChartSnapshot)]
    with MariaConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            select {", ".join(chart_fields)} from month_chart
            where year(date) = {year} and month(date) = {month}
            """
        )

    rows = list(cursor.fetchall())
    result = pandas.DataFrame(rows, columns=chart_fields)
    result.index = result['code']
    result = result.drop('code', axis=1)
    return result


def _snapshot(year: int, month: int) -> Iterator[Tuple]:
    with MariaConnection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
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
        )

    for record in cursor.fetchall():
        yield record


def snapshot_iterator(year: int, month: int) -> Iterator[ChartSnapshot]:
    for record in _snapshot(year, month):
        yield ChartSnapshot(
            code=record[0],
            date=record[1],
            open=record[2],
            high=record[3],
            low=record[4],
            close=record[5],
            vol=record[6],
            cap=record[7]
        )


def snapshot_dataframe(year: int, month: int) -> pandas.DataFrame:
    result = pandas.DataFrame([snap.__dict__ for snap in snapshot_iterator(year, month)])
    result.index = result['code']
    result = result.drop('code', axis=1)
    return result


def update_all_month_chart():
    """
    1996-01 부터 지난달까지 월 차트를 만들어 저장
    """
    today = date.today()
    year_month_list = []
    for year in range(2022, today.year + 1):
        if year == today.year:
            month_end = today.month - 1
        else:
            month_end = 12

        for month in range(1, month_end + 1):
            year_month_list.append((year, month))

    for year, month in year_month_list:
        update_month_chart(year, month)


def update_month_chart(year: int, month: int):
    print(f'Updating month_chart({year}/{month})')
    with MariaConnection() as connection:
        cursor = connection.cursor()
        values = [f"""('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]})"""
                  for row in _snapshot(year, month)]

        values_text = ",\n".join(values)
        insert_query = f"""
            insert into month_chart
            values
            {values_text};
        """
        n = cursor.execute(insert_query)
        print(f"{n} rows updated")
        connection.commit()
