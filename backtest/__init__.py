from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import *

import pandas as pd

from core import QuantFactor
from repository import get_day_chart, get_bussness_months
from repository.maria import corp
from util import YearMonth
from util import cagr, rate_of_return, N, R


@dataclass
class BackTestReport:
    """
    백테스트 개요 리포트
    """
    initial: float = 0
    final: float = 0
    cagr: float = 0
    mdd: float = 0
    events: pd.DataFrame = None
    cycles: pd.DataFrame = None

    def put_events(self, events: pd.DataFrame):
        self.events = pd.concat([self.events, events])

    def put_cycle(self, begin: YearMonth, revenue_rate: float, amount: float):
        cycle = pd.DataFrame({"수익률": revenue_rate, "평가액": amount}, index=[begin])
        self.cycles = pd.concat([self.cycles, cycle])


@dataclass
class Grade:
    amount: float
    max: float
    mdd: float

    def update(self):
        self.max = self.amount if self.amount > self.max else self.max
        dd = self.amount / self.max - 1
        self.mdd = dd if dd < self.mdd else self.mdd


# noinspection DuplicatedCode
class BackTest:

    def __init__(
        self,
        factor: Type[QuantFactor],
        from_date: date,
        to_date: date,
        sub_factors: List[Type[QuantFactor]] = None,
        p_w: float = 1.9,
        a_w: float = 0.9,
        portfolio_size=10,
        holding_period=1
    ):
        # 사용 지표
        self.factor = factor
        self.sub_factors = sub_factors if sub_factors else []
        self.major_colums = ["name", "수익률"]
        self.from_date, self.to_date = from_date, to_date
        self.pw, self.aw, self.portfolio_size = p_w, a_w, portfolio_size
        self.holding_period = holding_period
        self.events: Optional[pd.DataFrame] = None
        self.cycles: Optional[pd.DataFrame] = None

    def trade(self, today: date, nextday: date):
        before = get_day_chart(today)
        before = before[before['vol'] != 0]  # 거래량 미확인 종목 제외
        before = before[before['cap'] != 0]  # 시가총액 미확인 종목 제외
        # 매도일 주가데이터 조회
        after = get_day_chart(nextday)

        # 변동율
        change = rate_of_return(before['close'], after['close']).to_frame(name='수익률')

        # 팩터 계산
        df = self.factor.calc(today)
        for sub_factor in self.sub_factors:
            df = df.join(sub_factor.calc(today))

        df = df.join(before['close'].to_frame('매수가'))
        df = df.join(after['close'].to_frame('매도가'))
        df = df.join(change)

        # 데이터 누락종목 제외
        df = df.dropna()

        # Add ranking
        df['name'] = [corp.get_name(code) for code in df.index]

        df = df[self.major_colums + [col for col in df.columns if col not in self.major_colums]]
        return df.sort_values(by=self.factor.name, ascending=False)

    def run(self):
        factor_grade = Grade(amount=1, max=1, mdd=0)
        prev_date = None
        for this_date in get_bussness_months(self.from_date, self.to_date):
            if prev_date is None:
                prev_date = this_date
                continue

            df = self.trade(prev_date, this_date)
            holdings_corp_codes = [c.code for c in corp.get_holdings_corp()]
            df = df.filter(items=[code for code in df.index if code not in holdings_corp_codes], axis=0)  # 지주사 제거
            top = df[:self.portfolio_size]
            revenue_rate = top['수익률'].mean()
            factor_grade.amount *= (revenue_rate + 1)
            factor_grade.update()
            top.index = [[this_date] * len(top.index), top.index]
            top.index.names = ['매도일', '종목코드']
            top = top.join(top["수익률"].rank(ascending=False).to_frame(R("수익률")))
            self.events = pd.concat([self.events, top])
            self.cycles = pd.concat([
                self.cycles,
                pd.DataFrame({"수익률": revenue_rate, "평가액": factor_grade.amount, "벤치마크 수익률": df["수익률"].mean()},
                             index=[this_date]),
            ])
            self.cycles.index.names = ["date"]

            prev_date = this_date
            print(this_date, factor_grade.amount)

        self.make_report(initial=1, final_=factor_grade.amount, mdd=factor_grade.mdd)

        # todo: 성능 최적화
        # todo: KODEX 200과 비교
        # todo: deepsearch 데이터 DB에 넣어 보관

    def make_report(self, initial: float, final_: float, mdd):
        duration = (self.to_date - self.from_date).days / 365
        summary = pd.Series(
            {
                "from": self.from_date,
                "to": self.to_date,
                "duration": duration,
                "pw": self.pw,
                "aw": self.aw,
                "portfolio_size": self.portfolio_size,
                "holding_period": self.holding_period,
                "initial": 1,
                "final": final_,
                "cagr": cagr(initial, final_, duration),
                "mdd": mdd
            }
        )

        os.makedirs('.out', exist_ok=True)
        summary.to_csv('.out/summary.csv')

        self.cycles['outperform'] = self.cycles['수익률'] - self.cycles['벤치마크 수익률']
        self.cycles.to_csv(".out/cycles.csv")
        self.events.to_csv(".out/events.csv")

    def analysis(self):
        """deprecated"""
        # 지표 랭크별 수익률 평균, 수익률 랭크별
        revenue_by_factor_rank, power_by_revenue_rank = pd.DataFrame(), pd.DataFrame()
        for i in range(self.portfolio_size):
            rank = i + 1

            revenue_avg_per_factor_rank = self.events[self.events[R(self.factor.name)] == rank]['수익률'].mean()
            revenue_by_factor_rank = pd.concat([
                revenue_by_factor_rank,
                pd.DataFrame({"AVG(수익률)": revenue_avg_per_factor_rank}, index=[rank])
            ])

            power_avg_by_revenue_rank = self.events[self.events[R("수익률")] == rank][N(self.factor.name)].mean()
            power_by_revenue_rank = pd.concat([
                power_by_revenue_rank,
                pd.DataFrame({f"AVG(P({self.factor.name}))": power_avg_by_revenue_rank}, index=[rank])
            ])

        if revenue_by_factor_rank is not None:
            revenue_by_factor_rank.to_csv(".out/revenue_by_factor_rank.csv", index_label=[f"R({self.factor.name})"])

        if power_by_revenue_rank is not None:
            power_by_revenue_rank.to_csv(".out/power_by_revenue_rank.csv", index_label=["R(수익률)"])

        revenue_by_code = {}
        for i in self.events.index:
            code = i[1]
            data = self.events.loc[i]
            if code not in revenue_by_code:
                revenue_by_code.update({code: 0})

            revenue_by_code[code] += data['수익률']

        pd.DataFrame(
            {
                "name": [corp.get_name(code) for code in revenue_by_code.keys()],
                "revenue": revenue_by_code.values()
            },
            index=list(revenue_by_code.keys())).sort_values(by="revenue").to_csv(".out/revenue_by_code.csv")
