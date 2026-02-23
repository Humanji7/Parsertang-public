import asyncio
import inspect
import json
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)


class NativeWsClient:
    url: str = ""

    def build_subscribe(self, symbols: list[str]) -> dict:
        raise NotImplementedError

    def parse(self, msg: dict, ts_recv: int):
        raise NotImplementedError

    async def connect(self, symbols: list[str], on_event):
        backoff_s = 1.0
        max_backoff_s = 60.0
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, heartbeat=20) as ws:
                        backoff_s = 1.0
                        payload = self.build_subscribe(symbols)
                        if isinstance(payload, list):
                            for item in payload:
                                await ws.send_json(item)
                        else:
                            await ws.send_json(payload)

                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                ts_recv = int(time.time() * 1000)
                                if message.data == "ping":
                                    await ws.send_str("pong")
                                    continue
                                if message.data == "pong":
                                    continue
                                try:
                                    decoded = json.loads(message.data)
                                except Exception:
                                    continue
                                ev = self.parse(decoded, ts_recv)
                                if ev:
                                    result = on_event(ev)
                                    if inspect.isawaitable(result):
                                        await result
                            elif message.type == aiohttp.WSMsgType.BINARY:
                                ts_recv = int(time.time() * 1000)
                                ev = self.parse(message.data, ts_recv)
                                if ev:
                                    result = on_event(ev)
                                    if inspect.isawaitable(result):
                                        await result
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                err = None
                                try:
                                    err = ws.exception()
                                except Exception:  # noqa: BLE001
                                    err = None
                                logger.warning(
                                    "WSNATIVE closed | client=%s url=%s type=%s err=%r",
                                    self.__class__.__name__,
                                    self.url,
                                    getattr(message.type, "name", str(message.type)),
                                    err,
                                )
                                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "WSNATIVE reconnect | client=%s url=%s backoff=%.1fs err=%r",
                    self.__class__.__name__,
                    self.url,
                    backoff_s,
                    exc,
                )
                await asyncio.sleep(backoff_s)
                backoff_s = min(max_backoff_s, backoff_s * 2)
