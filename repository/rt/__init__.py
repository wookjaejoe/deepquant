import asyncio
from dataclasses import dataclass
from threading import Thread
from typing import *

import jsons
import websockets

from config import config


@dataclass
class StockRt:
    code: str
    name: str
    price: int
    cap: int
    control_kind: str
    supervision_kind: str
    status_kind: str


def fetch_all() -> List[StockRt]:
    async def _fetch():
        async with websockets.connect(config['stockrt']["url"]) as websocket:
            return jsons.loads(await websocket.recv(), List[StockRt])

    return asyncio.run(_fetch())


async def _subscribe_async(on_next: Callable[StockRt, None]):
    async with websockets.connect(config['stockrt']["url"] + "/subscribe") as websocket:
        while True:
            on_next(jsons.loads(await websocket.recv(), StockRt))


def _subscribe_sync(on_next: Callable[StockRt, None]):
    asyncio.run(_subscribe_async(on_next))


class StockRtSubscriber(Thread):
    def __init__(self, on_next: Callable[StockRt, None]):
        super().__init__()
        self.on_next = on_next

    def run(self):
        _subscribe_sync(self.on_next)
