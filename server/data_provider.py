import asyncio
import logging
from datetime import date

import jsons
import numpy as np
import pandas as pd
import websockets

from config import config
from repository import load_financial

logger = logging.getLogger("DataProvider")


class DataProvider:
    recipe = {
        "GP/P": 6,
        "1/P": 8,

        "GP_YoY": 1,
        "GP_QoQ": 2,
        "O_YoY": 1,
        "O_QoQ": 2,
    }

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop: asyncio.AbstractEventLoop = loop
        self.queue = asyncio.Queue()
        self.table = pd.DataFrame()

    def rerank(self):
        factors = ["GP_YoY", "GP_QoQ", "O_YoY", "O_QoQ"]

        for pos in ["A", "EQ"]:
            factor = f"{pos}/P"
            factors.append(factor)
            self.table[factor] = self.table[pos] / self.table["P"]
            self.table.loc[self.table[pos] <= 0, factor] = np.nan

        for neg in ["P", "A", "EQ"]:
            for pos in ["GP", "O", "E"]:
                factor = f"{pos}/{neg}"
                factors.append(factor)
                self.table[factor] = self.table[pos] / self.table[neg]
                self.table.loc[self.table[neg] <= 0, factor] = np.nan

        # factor - 1/P
        factors.append("1/P")
        self.table["1/P"] = 1 / self.table["P"]

        # 개별 팩터들의 pct 계산
        for factor in factors:
            colname_rank = f"{factor}_pct"
            self.table[colname_rank] = \
                np.ceil(self.table[factor].rank(method="min", pct=True) * 100)

        # super 팩터 계산
        factor = "super"
        factors.append(factor)
        self.table[factor] = sum([self.table[f"{k}_pct"] * v for k, v in DataProvider.recipe.items()]) * 100
        # fixme: pct로 할필요가 없음. sum(rank * weight) / sum(weight) 이게 더 좋을 것 같다. 이렇게 하면 1~전체 종목 개수로 집게 되지 않을까?
        self.table[f"{factor}_pct"] = np.ceil(self.table[factor].rank(method="min", pct=True) * 100)
        self.table[f"{factor}_rank"] = \
            np.ceil(self.table[factor].rank(ascending=False, method="min"))
        self.table = self.table.sort_values(factor, ascending=False)

    async def init_table(self):
        dest = config['stockrt']["url"]
        ws = await asyncio.wait_for(websockets.connect(dest), timeout=10)
        stocks = jsons.loads(await ws.recv())
        today = date.today()
        self.table = pd.DataFrame(stocks).set_index("code")
        self.table = self.table.join(load_financial(today.year, today.month))
        self.table.rename(columns={
            "cap": "P",
            "자산총계": "A",
            "자본총계": "EQ",
            "매출총이익": "GP",
            "영업이익": "O",
            "당기순이익": "E",
            "영업활동으로인한현금흐름": "CF"
        }, inplace=True)

    async def process(self):
        """
        시세 변경 사항 큐에 담긴 항목들을 모두 꺼내 처리함.
        """
        while True:
            if self.queue.empty():
                await asyncio.sleep(1)
                continue

            buffer = {}
            while not self.queue.empty():
                items = await self.queue.get()
                buffer.update({item["code"]: item for item in items})

            logger.info(f"Updating table for {len(buffer)} records...")
            new_data = pd.DataFrame(buffer.values()).set_index("code")
            self.table.update(new_data)
            self.rerank()

    async def listen(self):
        """
        Stock RT 서버로부터 시세 변경 이벤트를 지속 수신하고 큐에 담는다.
        """
        logger.info("Connecting to StockRT server...")
        dest = config['stockrt']["url"] + "/subscribe"
        websocket = await asyncio.wait_for(websockets.connect(dest), timeout=30)

        while True:
            data = await websocket.recv()
            data = jsons.loads(data)
            await self.queue.put(data)

    def init(self):
        self.loop.run_until_complete(self.init_table())
        self.loop.create_task(self.process())
        self.loop.create_task(self.listen())
