from __future__ import annotations

import logging
from datetime import datetime

from pymongo import MongoClient

from config import config

_logger = logging.getLogger(__file__)
_client = MongoClient(config["mongo"]["url"])


def _id(title: str, year: int, quarter: int = None):
    if quarter:
        return "_".join([title, str(year), str(quarter)])
    else:
        return "_".join([title, str(year)])


class DsCollection:
    col = _client["finance"]["ds"]

    @classmethod
    def exists(cls, title: str, year: int, quarter: int = None) -> bool:
        return cls.col.count_documents({"_id": _id(title, year, quarter)}) > 0

    @classmethod
    def insert_one(cls, raw: dict, title: str, year: int, quarter: int = None):
        assert quarter is None or quarter in [1, 2, 3, 4]
        # noinspection DuplicatedCode
        assert raw
        assert raw["success"] is True
        data = raw["data"]
        assert not data["exceptions"]
        subpod = data['pods'][0]['subpods'][0]
        assert subpod['class'] == 'Compiler:CompilationSucceeded'
        assert subpod['content']['data'][0] == 'Compilation succeeded.'

        raw["_id"] = _id(title, year, quarter)
        raw["title"] = title
        raw["year"] = year
        raw["quarter"] = quarter
        raw["updated"] = datetime.now()
        cls.col.insert_one(raw)

    @classmethod
    def fetch_one(cls, title: str, year: int, quarter: int = None) -> dict:
        return cls.col.find_one({"_id": _id(title, year, quarter)})
