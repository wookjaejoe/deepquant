import asyncio

from .base import DaemonAppConfig
import logging
from config import config
import websockets
import jsons
import pandas as pd
from core.strategy import recipe
from core.repository import load_financial
import numpy as np
from datetime import date

_logger = logging.getLogger(__name__)


# todo: apps.get_app_config(MyApp1Config.name)

class QuantAppConfig(DaemonAppConfig):
    name = 'apps.quant'

    def __init__(self, app_name, app_module):
        DaemonAppConfig.__init__(self, app_name, app_module)
        self.queue = asyncio.Queue()
        self.table = pd.DataFrame()

    def work(self):
        event_loop = asyncio.new_event_loop()
        event_loop.run_until_complete(self.init_table())
        event_loop.create_task(self.listen_market())
        event_loop.create_task(self.listen_queue())
        event_loop.run_forever()

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

    async def listen_market(self):
        """
        Stock RT 서버로부터 시세 변경 이벤트를 지속 수신하고 큐에 담는다.
        """
        dest = config['stockrt']["url"] + "/subscribe"
        _logger.info(f"Connecting websocket to {dest}")
        websocket = await asyncio.wait_for(websockets.connect(dest), timeout=30)

        while True:
            data = await websocket.recv()
            data = jsons.loads(data)
            await self.queue.put(data)

    async def listen_queue(self):
        while True:
            if self.queue.empty():
                await asyncio.sleep(1)
                continue

            buffer = {}
            while not self.queue.empty():
                items = await self.queue.get()
                buffer.update({item["code"]: item for item in items})

            _logger.info(f"Updating table for {len(buffer)} records...")
            new_data = pd.DataFrame(buffer.values()).set_index("code")
            self.table.update(new_data)
            self.rerank()
            await asyncio.sleep(0)

    def rerank(self):
        factors = ["GP_YoY", "GP_QoQ", "O_YoY", "O_QoQ"]

        def join_fraction_factor(_pos: str, _neg: str):
            _factor = f"{_pos}/{_neg}"
            self.table[_factor] = self.table[pos] / self.table[neg]
            self.table.loc[self.table[neg] <= 0, _factor] = np.nan
            factors.append(_factor)

        for pos in ["A", "EQ"]:
            for neg in ["P"]:
                join_fraction_factor(pos, neg)

        for pos in ["GP", "O", "E"]:
            for neg in ["P", "A", "EQ"]:
                join_fraction_factor(pos, neg)

        self.table["1/P"] = 1 / self.table["P"]
        self.table.loc[self.table[neg] <= 0, "1/P"] = np.nan
        factors.append("1/P")

        # 개별 팩터들의 pct 계산
        for factor in factors:
            colname_rank = f"{factor}_percentile"
            self.table[colname_rank] = self.table[factor].rank(method="min", pct=True)

        # super 팩터 계산
        factor = "super"
        factors.append(factor)
        self.table[factor] = sum([self.table[f"{k}_percentile"] * w for k, w in recipe.items()]) / sum(recipe.values())
        self.table[f"{factor}_percentile"] = self.table[factor].rank(method="min", pct=True)
        self.table[f"{factor}_rank"] = np.ceil(self.table[factor].rank(ascending=False, method="min"))
        self.table = self.table.sort_values(factor, ascending=False)
