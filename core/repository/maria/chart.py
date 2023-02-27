import pandas as pd

from base.timeutil import YearMonth
from .conn import MariaConnection, maria_home


def insert_month_chart(year: int, month: int):
    with MariaConnection() as conn:
        conn.query(f"""insert into month_chart (
        SELECT
            code,
            MAX(date)                                                                    as date,
            cast(SUBSTRING_INDEX(MIN(CONCAT(date, '_', open)), '_', -1) as unsigned)     as open,
            cast(MAX(high) as unsigned)                                                  as high,
            cast(MIN(low) as unsigned)                                                   as low,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', close)), '_', -1) as unsigned)    as close,
            cast(SUM(vol) as unsigned)                                                   as vol,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', vol)), '_', -1) as unsigned)      as vol_last,
            cast(SUM(val) as unsigned)                                                   as val,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', val)), '_', -1) as unsigned)      as val_last,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', cap)), '_', -1) as unsigned)      as cap,
            cast(SUBSTRING_INDEX(MAX(CONCAT(date, '_', shares)), '_', -1) as unsigned)   as shares
        FROM chart
        where year(date) = {year} and month(date) = {month}
        group by code, year(date), month(date)
        order by date);
        """)
        conn.commit()


def upload_month_chart():
    with MariaConnection() as conn:
        conn.query(
            """
            create table if not exists month_chart
            (
                code        varchar(8) null,
                date        date null,
                open        int null,
                high        int null,
                low         int null,
                close       int null,
                vol         bigint null,
                vol_last    bigint null,
                val         bigint null,
                val_last    bigint null,
                cap         bigint null,
                shares      bigint null,
                primary key (code, date)
            );
            """
        )
        conn.query("create index if not exists chart_code_index on month_chart (code);")
        conn.query("create index if not exists chart_date_index on month_chart (date);")
        conn.commit()

    dates = pd.read_sql("select distinct date from chart", maria_home())["date"]
    yms = sorted({YearMonth.from_date(d) for d in dates})
    assert len(yms) == len(set(yms))
    for ym in min(yms).to(max(yms)):
        print(ym)
        insert_month_chart(ym.year, ym.month)
