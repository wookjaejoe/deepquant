import asyncio
import logging

import websockets

_logger = logging.getLogger(__name__)


class WebsocketServer:
    def __init__(self, port):
        self.connections = set()
        self.host = '0.0.0.0'
        self.port = port

    def publish(self, msg: str):
        websockets.broadcast(self.connections, msg)

    async def __on_connect(self, websocket):
        self.connections.add(websocket)

        try:
            _logger.info(f'New connection. Total: {len(self.connections)}')
            # 세션 유지
            async for _ in websocket: pass
        finally:
            # Unregister user
            self.connections.remove(websocket)

    async def __start_async(self):
        async with websockets.serve(self.__on_connect, self.host, self.port):
            await asyncio.Future()  # run forever

    def start(self):
        asyncio.run(self.__start_async())
