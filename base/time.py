from __future__ import annotations

from dataclasses import dataclass
from typing import *
from datetime import date


@dataclass
class Quarter:
    year: int
    quarter: int

    def __str__(self):
        return f"{self.year}-{self.quarter}Q"

    def pre(self):
        if self.quarter == 1:
            return Quarter(self.year - 1, 4)
        else:
            return Quarter(self.year, self.quarter - 1)

    def minus(self, quarter: int) -> Quarter:
        result = self
        for quarter in range(quarter):
            result = result.pre()

        return result

    def iter_back(self, count) -> Iterator[Quarter]:
        for i in range(count):
            yield self.minus(i)

    @staticmethod
    def today():
        today = date.today()
        return Quarter(today.year, int(today.month / 4) + 1)


@dataclass
class YearMonth:
    year: int
    month: int

    @staticmethod
    def today():
        today = date.today()
        return YearMonth(today.year, today.month)

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

    def in_month(self) -> int:
        return self.year * 12 + self.month

    def duration(self, other) -> float:
        return (other.in_month() - self.in_month()) / 12

    def __gt__(self, other: YearMonth):
        return self.in_month().__gt__(other.in_month())

    def __lt__(self, other: YearMonth):
        return self.in_month().__lt__(other.in_month())

    def __ge__(self, other: YearMonth):
        return self.in_month().__ge__(other.in_month())

    def __le__(self, other: YearMonth):
        return self.in_month().__le__(other.in_month())

    def __str__(self):
        return "-".join([str(self.year).ljust(4, "0"), str(self.month).rjust(2, "0")])

    def __hash__(self):
        return hash(self.in_month())
