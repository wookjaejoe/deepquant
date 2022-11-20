from dataclasses import dataclass
from typing import *

from .conn import MariaConnection


@dataclass
class Corp:
    code: str
    name: str


def refetch_all() -> Iterator[Corp]:
    with MariaConnection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * from corp")

    return [Corp(code=record[0], name=record[1]) for record in cursor.fetchall()]


_cache: Dict[str, Corp] = {corp.code: corp for corp in refetch_all()}


def get_corps() -> Iterator[Corp]:
    return [v for v in _cache.values()]


def get_holdings_corp() -> Iterator[Corp]:
    for code, corp in _cache.items():
        name = corp.name
        if "지주" in name or "홀딩스" in name:
            yield corp


def get_name(code: str) -> str:
    return _cache[code].name


def exists(code: str) -> bool:
    return code in _cache.keys()
