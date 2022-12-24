import asyncio
import logging
from urllib.parse import urlparse, parse_qs

import jsons
import websockets

from .data_provider import DataProvider

_logger = logging.getLogger(__name__)


def parse_req(url: str):
    url = urlparse(url)
    qs = parse_qs(url.query)
    print()


class WebSocketServer:
    major_columns = ["code", "name", "price", "P", "control_kind", "supervision_kind", "status_kind"]
    major_columns += ["rws", "super", "super_rank"]
    major_columns += DataProvider.recipe.keys()
    major_columns += [f"{k}_rank" for k in DataProvider.recipe.keys()]

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        port: int = 8080
    ):
        self.loop = loop
        self.data_provider = DataProvider(loop)
        self.port = port

    async def send_head(self, session, params: dict):
        try:
            limit = int(params["limit"][0])
        except:
            limit = 100

        table = self.data_provider.table.copy()
        table = table.sort_values(by="super", ascending=False)[:limit]
        table["확정실적"] = str(table["확정실적"])
        table["code"] = table.index
        table = table[WebSocketServer.major_columns]
        res = jsons.dumps(table.T.to_dict().values(), allow_nan=False)
        await session.send(res)

    # noinspection PyTypeChecker
    async def _on_connect(self, session):
        try:
            _logger.info(f"New session - path: {session.path}, remote: {session.remote_address}")
            url = urlparse(session.path)
            if url.path.endswith("/head"):
                await self.send_head(session, parse_qs(url.query))

            # async for _ in session:
            #     pass  # 세션 유지
        except BaseException as e:
            _logger.warning(str(e))

    async def serve(self):
        await websockets.serve(self._on_connect, "0.0.0.0", self.port)

    def init(self):
        self.data_provider.init()
        self.loop.create_task(self.serve())
