import asyncio
import logging
from datetime import date, datetime, timezone
from threading import Thread

import jsons
import numpy as np
import pandas as pd
import websockets

from base.coding import Singleton
from config import config
from core.repository import load_financial
from core.strategy import recipe
from typing import *

_logger = logging.getLogger(__name__)


class QuantPicker(Singleton):
    major_colums = [
        "code",
        "name",
        "exchange",
        "price",
        "yesterday_close",
        "P",
        "control_kind",  # 감리구분: 정상, 주의, 경고, 위험예고, 위험
        "supervision_kind",  # 관리구분: 일반, 관리
        "status_kind",  # 주식상태: 정상, 거래정지, 거래중단
        "super",
        "super_percentile",
        "super_rank",
        "GP",
        "O",
        "E",
        *recipe.keys(),
        *[f"{k}_percentile" for k in recipe.keys()]
    ]

    colname_alias = {
        "cap": "P",
        "자산총계": "A",
        "자본총계": "EQ",
        "매출총이익": "GP",
        "영업이익": "O",
        "당기순이익": "E",
        "영업활동으로인한현금흐름": "CF"
    }

    def __init__(self):
        self.queue = asyncio.Queue()
        self.table = pd.DataFrame()
        self.updated = None
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None

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
        dest = config['stockrt']["url"]
        ws = await asyncio.wait_for(websockets.connect(dest), timeout=10)
        stocks = jsons.loads(await ws.recv())
        today = date.today()
        self.table = pd.DataFrame(stocks).set_index("code")
        self.table = self.table.join(load_financial(today.year, today.month))
        self.table.update(self.table[self.table["확정실적"].notna()]["확정실적"].apply(lambda x: str(x)))
        self.table.rename(columns=self.colname_alias, inplace=True)

    async def listen_market(self):
        """
        Stock RT 서버로부터 시세 변경 이벤트를 지속 수신하고 큐에 담는다.
        """
        dest = config['stockrt']["url"] + "/subscribe"
        while True:
            try:
                _logger.info(f"Connecting websocket to {dest}")
                self.websocket = await asyncio.wait_for(websockets.connect(dest), timeout=30)
                while True:
                    data = await self.websocket.recv()
                    data = jsons.loads(data)
                    await self.queue.put(data)
            except Exception as e:
                _logger.error("An error occured with websocket.", exc_info=e)
                await self.websocket.close()
                await asyncio.sleep(0)

    async def listen_queue(self):
        while True:
            if self.queue.empty():
                await asyncio.sleep(1)
                continue

            buffer = {}
            while not self.queue.empty():
                items = await self.queue.get()
                buffer.update({item["code"]: item for item in items})

            _logger.debug(f"Updating table for {len(buffer)} records...")
            new_data = pd.DataFrame(buffer.values()).set_index("code").rename(columns=self.colname_alias)
            self.table.update(new_data)
            self.rerank()
            self.updated = datetime.now(timezone.utc)
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

        factors.append("P")

        # 개별 팩터들의 pct 계산
        for factor in factors:
            colname_rank = f"{factor}_percentile"
            self.table[colname_rank] = self.table[factor].rank(method="min", pct=True)

        # super 팩터 계산
        factor = "super"
        factors.append(factor)
        sv = sum([self.table[f"{k}_percentile"] * w for k, w in recipe.items()])
        sn = (sv - sv.mean()) / sv.std()
        self.table[factor] = sn / sn.max()
        self.table[f"{factor}_percentile"] = self.table[factor].rank(method="min", pct=True)
        self.table[f"{factor}_rank"] = np.ceil(self.table[factor].rank(ascending=False, method="min"))
        self.table = self.table.sort_values(factor, ascending=False)
        print()

    def head(self, limit: int = 50) -> dict:
        table = self.table.copy()
        table = table.sort_values(by="super", ascending=False)[:limit]
        table["code"] = table.index
        return table.loc[:, self.major_colums].T.to_dict().values()

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
