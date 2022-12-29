import asyncio

import jsons
import websockets

from config import config


async def listen():
    print("Connecting to StockRT server...")
    dest = config['stockrt']["url"] + "/subscribe"
    websocket = await asyncio.wait_for(websockets.connect(dest), timeout=15)

    while True:
        data = await websocket.recv()
        data = jsons.loads(data)
        print(len(data), min([x["created"] for x in data]), max([x["created"] for x in data]))


asyncio.run(listen())
