import asyncio
import logging
from urllib.parse import urlparse, parse_qs

import jsons
import websockets

from .data_provider import DataProvider

_logger = logging.getLogger(__name__)


class WebSocketServer:
    major_colums = [
        "code",
        "name",
        "exchange",
        "price",
        "yesterday_close",
        "P",
        "control_kind",
        "supervision_kind",
        "status_kind",
        "super",
        "super_pct",
        "super_rank",
        *DataProvider.recipe.keys(),
        *[f"{k}_pct" for k in DataProvider.recipe.keys()]
    ]

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        port: int = 8080
    ):
        self.loop = loop
        self.data_provider = DataProvider(loop)
        self.port = port

    async def send_head(self, session, params: dict):
        limit = int(params["limit"][0]) if "limit" in params else 50
        table = self.data_provider.table.copy()
        table = table.sort_values(by="super", ascending=False)[:limit]
        table["확정실적"] = str(table["확정실적"])
        table["code"] = table.index
        table = table[self.major_colums]
        table.to_csv("pick.csv")
        res = jsons.dumps(table.T.to_dict().values(), allow_nan=False)  # fixme: allow_nan 뭔지 확인
        await session.send(res)

    # noinspection PyTypeChecker
    async def _on_connect(self, session):
        try:
            _logger.info(f"New session - path: {session.path}, remote: {session.remote_address}")
            url = urlparse(session.path)
            if url.path.endswith("/head"):
                await self.send_head(session, parse_qs(url.query))

            await session.wait_closed()
        except BaseException as e:
            _logger.warning(str(e))

    async def serve(self):
        await websockets.serve(self._on_connect, "0.0.0.0", self.port)
        # todo: publish to subscribers

    def init(self):
        self.data_provider.init()
        self.loop.create_task(self.serve())
