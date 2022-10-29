from .maria import chart
from pykrx import stock
from datetime import date
from pandas import DataFrame


def get_month_chart(year: int, month: int):
    result = chart.get_month_chart(year, month)
    if len(result.index) != 0:
        return result

    # fixme: replace to logging
    print("!" * 20)
    print(f"Chart({year}/{month}) not found.")
    print(f"Try fetching via pykrx.")
    print("!" * 20)

    last_business_day = stock.get_previous_business_days(year=date.today().year, month=date.today().month)[-1].date()
    df = stock.get_market_cap(last_business_day.strftime('%Y%m%d'))
    result = DataFrame(index=df.index)
    result = result.join(df['종가'].to_frame('close'))
    result = result.join(df['거래량'].to_frame('vol'))
    result = result.join(df['시가총액'].to_frame('cap'))
    return result
