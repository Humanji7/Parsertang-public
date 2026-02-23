import logging

import pytest

from parsertang.streams import Streams


@pytest.mark.asyncio
async def test_subscribe_orderbooks_logs_skip_when_zero_symbols(caplog):
    class DummyWsExchange:
        has = {"watchOrderBookForSymbols": False}

    streams = Streams.__new__(Streams)
    streams.exchanges = {"gate": DummyWsExchange()}
    streams.orderbook_limits = {}

    def on_update(ex_id: str, symbol: str, ob: dict) -> None:
        raise AssertionError("on_update should not be called when no symbols")

    with caplog.at_level(logging.INFO):
        await streams.subscribe_orderbooks({"gate": []}, on_update)

    assert "WS LEGACY | gate using per-symbol mode (0 symbols)" in caplog.text
    assert "WS SKIP | gate no symbols allocated" in caplog.text
