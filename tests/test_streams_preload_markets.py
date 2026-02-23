import asyncio
import logging

import pytest

from parsertang.streams import Streams


@pytest.mark.asyncio
async def test_subscribe_orderbooks_fails_fast_when_ws_markets_preload_fails(caplog):
    class DummyWsExchange:
        has = {"watchOrderBookForSymbols": False}

        def load_markets(self):
            raise Exception("network blocked")

        async def watch_order_book(self, symbol: str, limit: int = 20):
            raise AssertionError(
                "watch_order_book should not be called if preload fails"
            )

    streams = Streams.__new__(Streams)
    streams.exchanges = {"gate": DummyWsExchange()}
    streams.orderbook_limits = {}

    def on_update(ex_id: str, symbol: str, ob: dict) -> None:
        raise AssertionError("on_update should not be called if preload fails")

    with caplog.at_level(logging.INFO):
        task = asyncio.create_task(
            streams.subscribe_orderbooks({"gate": ["XRP/USDT"]}, on_update)
        )
        # Give the exchange_worker a chance to run preload and emit logs.
        for _ in range(50):
            if "WS MARKETS FAILED" in caplog.text:
                break
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert "WS MARKETS FAILED | gate: network blocked" in caplog.text


@pytest.mark.asyncio
async def test_ensure_markets_loaded_calls_open_for_preloaded_markets():
    class DummyWsExchange:
        def __init__(self):
            self.markets = {"BTC/USDT": {}}
            self.open_called = False

        def open(self):
            self.open_called = True

        def load_markets(self):
            raise AssertionError("load_markets should not be called for preloaded data")

    streams = Streams.__new__(Streams)
    ex = DummyWsExchange()

    ok = await streams._ensure_markets_loaded("kucoin", ex)

    assert ok is True
    assert ex.open_called is True
