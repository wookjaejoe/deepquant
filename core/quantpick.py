import asyncio
import logging
from datetime import date, datetime, timezone
from threading import Thread

import numpy as np
import pandas as pd

from base.coding import Singleton
from core.repository import load_financial
from core.repository.krx import get_ohlcv_latest
from core.strategy import recipe

_logger = logging.getLogger(__name__)


class QuantPicker(Singleton):
    major_colums = [
        "code",
        "name",
        "exchange",
        "price",
        "changesPct",
        "P",
        "super",
        "super_rank",
        "tags"
    ]

    colname_alias = {
        "cap": "P",
        "자산총계": "A",
        "자본총계": "EQ",
        "매출액": "R",
        "매출총이익": "GP",
        "영업이익": "O",
        "당기순이익": "E",
        "영업활동으로인한현금흐름": "CF"
    }

    def __init__(self):
        self.queue = asyncio.Queue()
        self.table = pd.DataFrame()
        self.updated = None

        # 반드시 생성자 마지막에 호출되어야 함
        thread = Thread(target=self.work, daemon=True)
        thread.start()

    def work(self):
        event_loop = asyncio.new_event_loop()
        event_loop.run_until_complete(self.init_table())
        _logger.info("QuantPicker table is ready.")
        event_loop.create_task(self.listen_market())
        event_loop.create_task(self.listen_queue())
        event_loop.run_forever()

    async def init_table(self):
        today = date.today()
        self.table = get_ohlcv_latest().set_index("code")
        self.table = self.table.join(load_financial(today.year, today.month))
        self.table.update(self.table[self.table["확정실적"].notna()]["확정실적"].apply(lambda x: str(x)))
        self.table.rename(columns=self.colname_alias, inplace=True)

    async def listen_market(self):
        """
        Stock RT 서버로부터 시세 변경 이벤트를 지속 수신하고 큐에 담는다.
        """

        while True:
            await self.queue.put(get_ohlcv_latest().to_dict("records"))
            await asyncio.sleep(60 * 10)

    async def listen_queue(self):
        while True:
            try:
                if self.queue.empty():
                    await asyncio.sleep(1)
                    continue

                buffer = {}
                while not self.queue.empty():
                    items = await self.queue.get()
                    buffer.update({item["code"]: item for item in items})

                new_data = pd.DataFrame(buffer.values()).set_index("code").rename(columns=self.colname_alias)
                _logger.info(f"{len(new_data)} changed. Re-ranking...")
                self.table.update(new_data)
                self.rerank()
                self.updated = datetime.now(timezone.utc)
            except Exception as e:
                _logger.error("An error occured while update for new items.", exc_info=e)

            await asyncio.sleep(0)

    def rerank(self):
        factors = set(recipe.keys())

        for x in ["R", "GP", "O", "E"]:
            for y in ["P", "A", "EQ"]:
                factor = f"{x}/{y}"
                self.table[factor] = self.table[x] / self.table[y]
                factors.add(factor)

        self.table["EQ/P"] = self.table["EQ"] / self.table["P"]

        # 개별 팩터들의 pct 계산
        for factor in factors:
            colname_rank = f"{factor}_pct"
            self.table[colname_rank] = self.table[factor].rank(method="min", pct=True)

        def weighted(pct: float, w: float):
            assert w != 0
            return pct * w if w > 0 else (1 - pct) * abs(w)

        # super 팩터 계산
        # 1. 레시피를 구성하는 개별 팩터 분위(percentile) * 가중치의 총합을 구함
        sv = sum([weighted(self.table[f"{k}_pct"], w) for k, w in recipe.items()])
        # 2. 위의 시리즈에 가중치의 총합을 나눈다 => 0~1 사이 값으로 일반화됨
        sv = sv / sum([abs(w) for w in recipe.values()])
        # 3. 표준정규화
        self.table["super"] = sv
        self.table["super_rank"] = np.ceil(self.table["super"].rank(ascending=False, method="min"))

        self.table["tags"] = ""
        self.attach_tag(self.table["R/A_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["GP/A_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["O/A_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["E/A_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["GP/EQ_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["GP/EQ_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["O/EQ_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["E/EQ_pct"] < 0.10, "낮은 수익성")
        self.attach_tag(self.table["open"] == 0, "거래정지")
        self.table = self.table.sort_values("super", ascending=False)
        # self.table.to_csv("pick.csv")

    def attach_tag(self, selector, tag: str):
        selector = selector & ~self.table["tags"].str.contains(tag)
        self.table.loc[selector, "tags"] += f"{tag};"

    def head(self, limit: int = 50) -> list:
        table = self.table.copy()
        table = table.sort_values(by="super", ascending=False)[:limit]
        table["code"] = table.index
        table["price"] = table["close"]
        return list(table.loc[:, self.major_colums].T.to_dict().values())

    def get(self, code: str) -> dict:
        result = self.table.loc[code].to_dict()
        result.update({"code": code})
        return result

    def distribution(self, colname: str):
        table = self.table[colname].dropna()
        return {
            "code": table.index.tolist(),
            "values": table.values.tolist(),
            "percentiles": table.rank(pct=True).tolist()
        }
