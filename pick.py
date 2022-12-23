import asyncio
from datetime import date
from typing import *

import jsons
import numpy as np
import pandas as pd
import websockets

from config import config
from repository import load_financial
from repository.rt import StockCurrent

rank_scale = 100


class RealTimeFactorPublisher:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop: asyncio.AbstractEventLoop = loop
        self.queue = asyncio.Queue()
        self.table = pd.DataFrame()
        self._terminate = None

    def calc_super(self, factor: str, factor_analysis: pd.Series):
        factor_weight = pow(-factor_analysis["spearman"] * factor_analysis["h5"], 1)
        print(factor, factor_weight)
        return self.table[f"{factor}_power"] * factor_weight

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

        for factor in factors:
            colname_rank = f"{factor}_rank"
            colname_power = f"{factor}_power"
            self.table[colname_rank] = \
                np.ceil(self.table[factor].rank(ascending=False, method="min", pct=True) * rank_scale)
            self.table[colname_power] = (self.table[factor] - self.table[factor].mean()) / self.table[factor].std()

        recipe = {
            "GP/P": 10,
            "1/P": 24,

            "GP_YoY": 1,
            "GP_QoQ": 2,
            "O_YoY": 1,
            "O_QoQ": 2,
        }

        # factor - super
        factors.append("super")
        factor = f"super"
        self.table[f"rws"] = sum([self.table[f"{k}_rank"] * v for k, v in recipe.items()])
        self.table[factor] = 1 / sum([self.table[f"{k}_rank"] * v for k, v in recipe.items()]) * 100
        self.table[f"{factor}_rank"] = \
            np.ceil(self.table[factor].rank(ascending=False, method="min", pct=True) * rank_scale)

        major_col = ["name", "price", "P", "control_kind", "supervision_kind", "status_kind"] \
                    + ["rws", "super", "super_rank"] \
                    + [f"{k}_rank" for k in recipe.keys()]

        result = self.table[~self.table["name"].str.endswith("홀딩스")]
        result = result[~result["name"].str.endswith("지주")]
        result = result.sort_values("super", ascending=False)[major_col]
        result.to_csv("pick.csv")
        print()

    async def init_table(self):
        dest = config['stockrt']["url"]
        ws = await asyncio.wait_for(websockets.connect(dest), timeout=1)
        stocks = jsons.loads(await ws.recv(), List[StockCurrent])
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
        print()

    async def process(self):
        while True:
            buffer = []
            while True:
                try:
                    buffer.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # todo: 처리
            # todo: 데이터 프레임에 데이터 업데이트명

            if buffer:
                new_data = pd.DataFrame(buffer).set_index("code")
                self.table.update(new_data)
                self.rerank()
                # todo: 변경 사항 publish - self.table.loc[new_data]

            await asyncio.sleep(1)

    async def listen(self):
        print("Connecting to StockRT server...")
        dest = config['stockrt']["url"] + "/subscribe"
        websocket = await asyncio.wait_for(websockets.connect(dest), timeout=1)
        print("Websocket connected.")

        while True:
            data = await websocket.recv()
            data = jsons.loads(data, StockCurrent)
            self.queue.put_nowait(data)

    def run(self):
        self.loop.run_until_complete(self.init_table())
        self.loop.create_task(self.process())
        self.loop.create_task(self.listen())

        def custom_exception_handler(loop, context):
            loop.default_exception_handler(context)
            loop.stop()

        self.loop.set_exception_handler(custom_exception_handler)
        self.loop.run_forever()


def main():
    RealTimeFactorPublisher(asyncio.new_event_loop()).run()


if __name__ == '__main__':
    main()
