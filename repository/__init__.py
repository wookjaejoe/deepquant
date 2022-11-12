from .maria import chart
from .maria.chart import get_bussness_dates, get_bussness_months
from datetime import date
from pandas import DataFrame


# fixme: deprecated
def _get_chart_via_pykrx(d: date):
    print("!" * 20)
    print(f"Try fetching via pykrx.")
    print("!" * 20)

    df = stock.get_market_cap(d.strftime('%Y%m%d'))
    result = DataFrame(index=df.index)
    result = result.join(df['종가'].to_frame('close'))
    result = result.join(df['거래량'].to_frame('vol'))
    return result.join(df['시가총액'].to_frame('cap'))


def get_day_chart(d: date):
    return chart.get_day_chart(d)


def get_month_chart(year: int, month: int):
    return chart.get_month_chart(year, month)
