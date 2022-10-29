from __future__ import annotations

import os
from typing import *

import pandas as pd

import repository.deepsearch as ds
from base import YearMonth
from repository import get_month_chart
from repository.maria import corp
from .util import *

from dataclasses import dataclass


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


# noinspection DuplicatedCode
class GppaBackTest:
    indicator = "GP/PA"
    major_colums = [R(indicator), "name", "수익률"]

    def __init__(
        self,
        from_ym: YearMonth,
        to_ym: YearMonth,
        pw: float = 1.9,
        aw: float = 0.9,
        portfolio_size=10,
        holding_period=1
    ):
        # 사용 지표
        self.from_ym, self.to_ym = from_ym, to_ym
        self.pw, self.aw, self.portfolio_size = pw, aw, portfolio_size
        self.holding_period = holding_period
        self.events: Optional[pd.DataFrame] = None
        self.cycles: Optional[pd.DataFrame] = None

    def pick(self, ym: YearMonth, exclude_codes: Set[str] = None):
        ym_after = ym.plus(self.holding_period)
        if ym_after > YearMonth.today():
            ym_after = YearMonth.today()

        before = get_month_chart(ym.year, ym.month)
        # 지정 종목 제외
        if exclude_codes:
            before = before.filter(items=[code for code in before.index if code not in exclude_codes], axis=0)
        before = before[before.apply(lambda x: x['cap'] != 0, axis=1)]  # 시가총액 미확인 종목 제외
        before = before[before.apply(lambda x: x['vol'] != 0, axis=1)]  # 거래량 미확인 종목 제외
        # 내년 차트 조회
        after = get_month_chart(ym_after.year, ym_after.month)
        after = after[after.apply(lambda x: x['cap'] != 0, axis=1)]  # 시가총액 미확인 종목 제외
        after = after[after.apply(lambda x: x['vol'] != 0, axis=1)]  # 거래량 미확인 종목 제외

        if len(after.index) == 0:
            after = before

        # 변동율
        change = ((after['close'] - before['close']) / before['close']).to_frame(name='수익률')

        # before: o, after: x -> 거래정지
        only_left = before.index.join(after.index, how='outer').drop(before.index.join(after.index, how='right'))
        for code in only_left:
            change.loc[code] = -0.99  # 거래중단 -> 수익률 -100% 가정

        # 재무데이터 로드
        df = sum(ds.load('매출액', ym.year, ym.month, 4))
        df = pd.merge(df, sum(ds.load('매출원가', ym.year, ym.month, 4)), left_index=True, right_index=True)
        df = pd.merge(df, sum(ds.load('당기순이익', ym.year, ym.month, 4)), left_index=True, right_index=True)
        df = pd.merge(df, ds.load_one('자산', ym.year, ym.month), left_index=True, right_index=True)

        # 코드 표준화
        df.index = [symbol.split(':')[1] for symbol in df.index]
        df.index.names = ['code']
        df['매출총이익'] = df['매출액'] - df['매출원가']
        df['GP/PA'] = df['매출총이익'] / ((before['cap'] ** self.pw) * (df['자산'] ** self.aw))
        df_indi = df[self.indicator]
        df = df.join(((df_indi - df_indi.mean()) / df_indi.std()).to_frame(P(self.indicator)))
        df = df.join(before['close'].to_frame(f'매수가'))
        df = df.join(after['close'].to_frame(f'매도가'))
        df = df.join(before['cap'].to_frame(f'cap'))

        # 수익률 칼럼 추가
        df = pd.merge(df, change, left_index=True, right_index=True)

        # nan value 제거
        df = df.dropna()

        # 종목명에 홀딩스, 지주 포함 종목 제거
        filtered_index = [code for code in df.index if
                          corp.exists(code) and '지주' not in corp.get_name(code) and '홀딩스' not in corp.get_name(code)]
        df = df.filter(items=filtered_index, axis=0)

        # Add ranking
        df = df.join(df[self.indicator].rank(ascending=False).to_frame(R(self.indicator)))
        df['name'] = [corp.get_name(code) for code in df.index]

        df = df[self.major_colums + [col for col in df.columns if col not in self.major_colums]]
        return df.sort_values(by=self.indicator, ascending=False)

    def run(self):
        my_amount, my_mdd = 1, 0
        top_max = my_amount
        exclude = set()
        for ym in self.from_ym.iter(to=self.to_ym, step=self.holding_period):
            df = self.pick(ym, exclude)
            top = df[:self.portfolio_size].sort_values(by=f"수익률", ascending=False)
            revenue_rate = top['수익률'].mean()
            my_amount = (my_amount * (revenue_rate + 1))

            # cycle report...
            top.index = [[ym] * len(top.index), top.index]
            top.index.names = ['시점', '종목코드']
            top = top.join(top["수익률"].rank(ascending=False).to_frame(R("수익률")))
            self.events = pd.concat([self.events, top])

            cycle = pd.DataFrame({"수익률": revenue_rate, "평가액": my_amount}, index=[ym])
            self.cycles = pd.concat([self.cycles, cycle])

            if my_amount > top_max:
                top_max = my_amount

            top_dd = my_amount / top_max - 1
            if top_dd < my_mdd:
                my_mdd = top_dd

        duration = self.from_ym.duration(self.to_ym)
        os.makedirs('.out', exist_ok=True)

        pd.Series(
            {
                "from_ym": self.from_ym,
                "to_ym": self.to_ym,
                "duration": duration,
                "pw": self.pw,
                "aw": self.aw,
                "portfolio_size": self.portfolio_size,
                "holding_period": self.holding_period,
                "initial": 1,
                "final": my_amount,
                "cagr": cagr(1, my_amount, duration),
                "mdd": my_mdd
            }
        ).to_csv('.out/summary.csv')
        self.cycles.to_csv(".out/cycles.csv")
        self.events.to_csv(".out/events.csv")

        # 지표 랭크별 수익률 평균
        revenue_by_indicator_rank = pd.DataFrame()
        # 수익률 랭크별
        power_by_revenue_rank = pd.DataFrame()

        for i in range(self.portfolio_size):
            rank = i + 1

            revenue_avg_per_indicator_rank = self.events[self.events[R(self.indicator)] == rank]['수익률'].mean()
            revenue_by_indicator_rank = pd.concat(
                [
                    revenue_by_indicator_rank,
                    pd.DataFrame({"AVG(수익율)": revenue_avg_per_indicator_rank}, index=[rank])
                ]
            )

            power_avg_by_revenue_rank = self.events[self.events[R("수익률")] == rank][P(self.indicator)].mean()
            power_by_revenue_rank = pd.concat(
                [
                    power_by_revenue_rank,
                    pd.DataFrame({f"AVG(P({self.indicator}))": power_avg_by_revenue_rank}, index=[rank])
                ]
            )

        if revenue_by_indicator_rank is not None:
            revenue_by_indicator_rank.to_csv(
                ".out/revenue_by_indicator_rank.csv",
                index_label=[f"R({self.indicator})"]
            )

        if power_by_revenue_rank is not None:
            power_by_revenue_rank.to_csv(
                ".out/power_by_revenue_rank.csv",
                index_label=["R(수익률)"]
            )

        # todo: 분석 보충
        # todo: logging
        # todo: 성능 최적화
        # todo: KODEX 200과 비교
        # todo: deepsearch 데이터 DB에 넣어 보관
        # todo: 적자 기업 제외 해볼 것
        # todo: 영업이익, 당기순이익 뭐가 다른지
