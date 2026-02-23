import asyncio
from types import SimpleNamespace

import aiohttp
import pytest

from parsertang.ws_native.client_base import NativeWsClient


class DummyWs:
    def __init__(self, messages: list[SimpleNamespace]):
        self._iter = iter(messages)
        self.sent_str: list[str] = []
        self.sent_json: list[object] = []

    async def send_json(self, payload):
        self.sent_json.append(payload)

    async def send_str(self, data: str):
        self.sent_str.append(data)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class DummyWsContext:
    def __init__(self, ws: DummyWs):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_ws_native_replies_to_text_ping_with_pong(monkeypatch) -> None:
    attempts = {"n": 0}
    ws = DummyWs(
        [
            SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data="ping"),
            SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None),
        ]
    )

    class DummySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def ws_connect(self, url, heartbeat=20):  # noqa: ARG002
            attempts["n"] += 1
            if attempts["n"] >= 2:
                raise asyncio.CancelledError()
            return DummyWsContext(ws)

    import parsertang.ws_native.client_base as client_base

    monkeypatch.setattr(client_base.aiohttp, "ClientSession", lambda: DummySession())

    class DummyClient(NativeWsClient):
        url = "wss://example"

        def build_subscribe(self, symbols):  # noqa: ARG002
            return {"op": "subscribe", "args": []}

        def parse(self, msg, ts_recv):  # noqa: ARG002
            return None

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(
                DummyClient().connect(["BTC/USDT"], on_event=lambda ev: None)
            )
    finally:
        loop.close()

    assert ws.sent_str == ["pong"]
