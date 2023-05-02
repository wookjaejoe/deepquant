from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import *
from datetime import date
from abc import ABCMeta
import calendar


class Valuable(metaclass=ABCMeta):

    @abc.abstractmethod
    def value(self) -> int: ...

    @staticmethod
    @abc.abstractmethod
    def from_value(value: int) -> Valuable: ...

    def plus(self, amount: int = 1):
        return self.from_value(self.value() + amount)

    def minus(self, amount: int = 1):
        return self.from_value(self.value() - amount)

    def diff(self, other: Valuable) -> int:
        return self.value() - other.value()

    def to(self, other: Valuable):
        item = self
        while item <= other:
            yield item
            item = item.plus()

    def __gt__(self, other: Valuable):
        return self.value().__gt__(other.value())

    def __lt__(self, other: Valuable):
        return self.value().__lt__(other.value())

    def __ge__(self, other: Valuable):
        return self.value().__ge__(other.value())

    def __le__(self, other: Valuable):
        return self.value().__le__(other.value())

    def __hash__(self):
        return hash(self.value())


@dataclass
class YearQuarter(Valuable):
    year: int
    quarter: int

    def __str__(self):
        return f"{self.year}-{self.quarter}Q"

    @staticmethod
    def today():
        today = date.today()
        return YearQuarter(today.year, int(today.month / 4) + 1)

    def value(self) -> int:
        return self.year * 4 + self.quarter

    @staticmethod
    def from_value(value: int) -> YearQuarter:
        if value % 4 == 0:
            return YearQuarter(int(value / 4) - 1, 4)
        else:
            return YearQuarter(int(value / 4), value % 4)

    @staticmethod
    def last_confirmed(year: int, month: int) -> YearQuarter:
        """
        입력된 년월의 말일에 확인 가능한 가장 최근 확정 실적 분기를 반환한다.

        https://dart.fss.or.kr/info/main.do?menu=410
        12월 결산 기준 정기보고서 제출 기한
        - 1/4분기  05/15
        - 반기보고서 08/14
        - 3/4분기  11/14
        - 사업보고서 03/31
        ----
        1월말  - 작년 3Q, 2Q, 1Q, 제작년 4Q
        2월말  - 작년 3Q, 2Q, 1Q, 제작년 4Q
        3월말  - 작년 3Q, 2Q, 1Q, 제작년 4Q
        4월말  - 작년 4Q, 3Q, 2Q, 1Q
        5월말  - 1Q, 작년 4Q, 3Q, 2Q
        6월말  - 1Q, 작년 4Q, 3Q, 2Q
        7월말  - 1Q, 작년 4Q, 3Q, 2Q
        8월말  - 2Q, 1Q, 작년 4Q, 3Q
        9월말  - 2Q, 1Q, 작년 4Q, 3Q
        10월말 - 2Q, 1Q, 작년 4Q, 3Q
        11월말 - 3Q, 2Q, 1Q, 작년 4Q
        12월말 - 3Q, 2Q, 1Q, 작년 4Q
        """

        if month in [1, 2, 3]:
            return YearQuarter(year - 1, 3)
        elif month in [4]:
            return YearQuarter(year - 1, 4)
        elif month in [5, 6, 7]:
            return YearQuarter(year, 1)
        elif month in [8, 9, 10]:
            return YearQuarter(year, 2)
        elif month in [11, 12]:
            return YearQuarter(year, 3)
        else:
            raise Exception(f"Invalid month: {month}")


@dataclass
class YearMonth(Valuable):
    year: int
    month: int

    def value(self) -> int:
        return self.year * 12 + self.month

    @staticmethod
    def from_value(value: int) -> YearMonth:
        if value % 12 == 0:
            return YearMonth(int(value / 12) - 1, 12)
        else:
            return YearMonth(int(value / 12), value % 12)

    @staticmethod
    def from_date(date_: date):
        return YearMonth(date_.year, date_.month)

    @staticmethod
    def from_string(s: str):
        sp = s.split("-")
        assert len(sp) == 2
        return YearMonth(int(sp[0]), int(sp[1]))

    @staticmethod
    def today():
        today = date.today()
        return YearMonth(today.year, today.month)

    def first_date(self) -> date:
        return date(self.year, self.month, 1)

    def last_date(self) -> date:
        return date(self.year, self.month, calendar.monthrange(self.year, self.month)[1])

    def duration(self, other) -> float:
        return (other.value() - self.value()) / 12

    def next(self):
        return self.from_value(self.value() + 1)

    def to(self, end: YearMonth) -> Iterable[YearMonth]:
        item = self
        while item <= end:
            yield item
            item = item.next()

    def __str__(self):
        return "-".join([str(self.year).ljust(4, "0"), str(self.month).rjust(2, "0")])

    def __hash__(self):
        return hash(self.value())
