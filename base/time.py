from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import *
from datetime import date
from abc import ABCMeta
import calendar


class Comparable(metaclass=ABCMeta):
    @abc.abstractmethod
    def value(self) -> int: ...

    def __gt__(self, other: YearMonth):
        return self.value().__gt__(other.value())

    def __lt__(self, other: YearMonth):
        return self.value().__lt__(other.value())

    def __ge__(self, other: YearMonth):
        return self.value().__ge__(other.value())

    def __le__(self, other: YearMonth):
        return self.value().__le__(other.value())

    def __hash__(self):
        return hash(self.value())


@dataclass
class Quarter(Comparable):
    year: int
    quarter: int

    def __str__(self):
        return f"{self.year}-{self.quarter}Q"

    def pre(self) -> Quarter:
        if self.quarter == 1:
            return Quarter(self.year - 1, 4)
        else:
            return Quarter(self.year, self.quarter - 1)

    def next(self) -> Quarter:
        if self.quarter == 4:
            return Quarter(self.year + 1, 1)
        else:
            return Quarter(self.year, self.quarter + 1)

    def minus(self, num: int) -> Quarter:
        result = self
        for _ in range(num):
            result = result.pre()

        return result

    def plus(self, num: int) -> Quarter:
        result = self
        for num in range(num):
            result = result.next()

        return result

    def iter_back(self, count) -> Iterator[Quarter]:
        for i in range(count):
            yield self.minus(i)

    def to(self, to: Quarter):
        q = self
        while to.value() >= q.value():
            yield q
            q = q.next()

    @staticmethod
    def today():
        today = date.today()
        return Quarter(today.year, int(today.month / 4) + 1)

    def value(self) -> int:
        return self.year * 4 + self.quarter

    @staticmethod
    def last_confirmed(year: int, month: int):
        """
        1월 - 작년 3Q, 2Q, 1Q, 제작년 4Q
        2월 - 작년 3Q, 2Q, 1Q, 제작년 4Q
        3월 - 작년 3Q, 2Q, 1Q, 제작년 4Q
        4월 - 작년 4Q, 3Q, 2Q, 1Q
        5월 - 작년 4Q, 3Q, 2Q, 1Q
        6월 - 1Q, 작년 4Q, 3Q, 2Q
        7월 - 1Q, 작년 4Q, 3Q, 2Q
        8월 - 1Q, 작년 4Q, 3Q, 2Q
        9월 - 2Q, 1Q, 작년 4Q, 3Q
        10월 - 2Q, 1Q, 작년 4Q, 3Q
        11월 - 2Q, 1Q, 작년 4Q, 3Q
        12월 - 3Q, 2Q, 1Q, 작년 4Q
        """

        if month in [1, 2, 3]:
            return Quarter(year - 1, 3)
        elif month in [4, 5]:
            return Quarter(year - 1, 4)
        elif month in [6, 7, 8]:
            return Quarter(year, 1)
        elif month in [9, 10, 11]:
            return Quarter(year, 2)
        elif month in [12]:
            return Quarter(year, 3)
        else:
            raise Exception(f"Invalid month: {month}")


@dataclass
class YearMonth(Comparable):
    year: int
    month: int

    @staticmethod
    def of(date_: date):
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

    def pre(self):
        if self.month == 1:
            return YearMonth(self.year - 1, 12)
        else:
            return YearMonth(self.year, self.month - 1)

    def next(self):
        if self.month == 12:
            return YearMonth(self.year + 1, 1)
        else:
            return YearMonth(self.year, self.month + 1)

    def plus(self, month: int):
        cursor = self
        for i in range(month):
            cursor = cursor.next()
        return cursor

    def minus(self, month: int):
        result = self
        for quarter in range(month):
            result = result.pre()

        return result

    def iter(self, to: YearMonth, step: int = 1):
        cursor = self
        while cursor <= to:
            yield cursor
            cursor = cursor.plus(step)

    def duration(self, other) -> float:
        return (other.value() - self.value()) / 12

    def value(self) -> int:
        return self.year * 12 + self.month

    def __str__(self):
        return "-".join([str(self.year).ljust(4, "0"), str(self.month).rjust(2, "0")])

    def __hash__(self):
        return hash(self.value())
