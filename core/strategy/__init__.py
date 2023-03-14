from __future__ import annotations

from dataclasses import dataclass
from typing import *
from enum import Enum

recipe = {
    "P": -4,
    "GP/P": 4,
    "EQ/P": 2,
    "GP_QoQ": 1,
    "O_QoQ": 1,
    "GP/A_QoQ": 1,
    "O/A_QoQ": 1,
}


class FactorUnit(Enum):
    CURRENCY = "CURRENCY"
    PERCENTILE = "PERCENTILE"
    PERCENTAGE = "PERCENTAGE"
    NONE = "NONE"

    @staticmethod
    def of(text: str) -> Optional[FactorUnit]:
        for element in FactorUnit:
            if text.upper() in [element.name.upper(), element.value.upper()]:
                return element

        return None


@dataclass
class Factor:
    alias: str
    fullname: str
    unit: FactorUnit


factor_candis = [
    Factor(
        alias="GP/P",
        fullname="매출총이익/시가총액",
        unit=FactorUnit.NONE),
    Factor(
        alias="P",
        fullname="시가총액",
        unit=FactorUnit.CURRENCY),
    Factor(
        alias="GP_YoY",
        fullname="매출총이익 YoY",
        unit=FactorUnit.PERCENTILE
    ),
    Factor(
        alias="GP_QoQ",
        fullname="매출총이익 QoQ",
        unit=FactorUnit.PERCENTILE
    ),
    Factor(
        alias="O_YoY",
        fullname="영업이익 YoY",
        unit=FactorUnit.PERCENTILE
    ),
    Factor(
        alias="O_QoQ",
        fullname="영업이익 QoQ",
        unit=FactorUnit.PERCENTILE
    )
]
