from __future__ import annotations

from datetime import date

import pandas as pd

from factor import latest
from repository.maria import corp, chart
from base import rate_of_return, cagr, mdd, YearMonth


class BackTest:
    MAJOR_COLNAMES = ["name", "수익률", "매수일자", "매도일자", "매수가", "매도가"]

    def __init__(
            self,
            begin: YearMonth,
            end: YearMonth,
            port_size=10,
    ):
        # 사용 지표
        self.factor = latest
        self.begin, self.end = begin, end
        self.port_size = port_size

    def run(self) -> pd.DataFrame:
        prev_date = None
        events = pd.DataFrame()
        cycles = pd.DataFrame()

        chart.month_chart(self.begin, self.end)["date"].unique()
        for this_date in get_bussness_months(self.begin, self.end):
            print(this_date)
            if not prev_date:
                prev_date = this_date
                continue

            df = self._on_date(prev_date, this_date).sort_values(by="factor", ascending=False)
            top = df[:self.port_size]
            events = pd.concat([events, top])

            revenue_portfolio = top["수익률"].mean()
            revenue_benchmark = df["수익률"].mean()
            revenue_outperform = revenue_portfolio - revenue_benchmark

            cycles = pd.concat([
                cycles,
                pd.DataFrame({
                    "portfolio": revenue_portfolio,
                    "benchmark": revenue_benchmark,
                    "outperform": revenue_outperform
                }, index=[this_date])
            ])

            prev_date = this_date

        self._report(events, cycles)
        return events

    def _on_date(self, today: date, nextday: date) -> pd.DataFrame:
        # todo: 생존 편향 해결
        before = chart.get_day_chart(today)
        before = before[before['vol'] != 0]  # 거래량 미확인 종목 제외
        before = before[before['cap'] != 0]  # 시가총액 미확인 종목 제외
        # 매도일 주가데이터 조회
        after = chart.get_day_chart(nextday)

        # 변동율
        change = rate_of_return(before['close'], after['close']).to_frame(name='수익률')

        # 팩터 계산
        df = self.factor.calc(today)
        df["매수일자"] = today
        df["매도일자"] = nextday
        df = df.join(before['close'].to_frame('매수가'))
        df = df.join(after['close'].to_frame('매도가'))
        df = df.join(change)

        # 데이터 누락종목 제외
        df = df.dropna()

        # Add ranking
        df['name'] = [corp.get_name(code) for code in df.index]
        return df[self.MAJOR_COLNAMES + [col for col in df.columns if col not in self.MAJOR_COLNAMES]]

    def _report(self, events: pd.DataFrame, cycles: pd.DataFrame):
        years = round((self.end - self.begin).days / 365, 2)
        cycles_portfolio_cumprod = (cycles["portfolio"] + 1).cumprod()
        cycles_benchmark_cumprod = (cycles["benchmark"] + 1).cumprod()
        pd.Series({
            "begin": self.begin,
            "end": self.end,
            "years": years,
            "cagr": cagr(initial=1, last=cycles_portfolio_cumprod[-1], years=years),
            "mdd": mdd(list(cycles_portfolio_cumprod.index), list(cycles_portfolio_cumprod.values))
        }).to_csv(".out/summary.csv")

        cycles["cycles_portfolio_cumprod"] = cycles_portfolio_cumprod
        cycles["cycles_benchmark_cumprod"] = cycles_benchmark_cumprod
        cycles.to_csv(".out/cycles.csv")
        events.to_csv(".out/events.csv")
