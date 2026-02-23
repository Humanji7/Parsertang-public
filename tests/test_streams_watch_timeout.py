import asyncio
import logging

import pytest

from parsertang.streams import Streams


@pytest.mark.asyncio
async def test_subscribe_orderbooks_logs_ws_timeout_when_watch_hangs(
    caplog, monkeypatch
):
    class DummyWsExchange:
        has = {"watchOrderBookForSymbols": False}

        def load_markets(self):
            return {"XRP/USDT": {}}

        async def watch_order_book(self, symbol: str, limit: int = 20):
            await asyncio.Event().wait()

    real_sleep = asyncio.sleep

    async def fast_sleep(_delay: float):
        await real_sleep(0)

    streams = Streams.__new__(Streams)
    streams.exchanges = {"gate": DummyWsExchange()}
    streams.orderbook_limits = {}
    streams.init_status = {"gate": {"status": "ok"}}

    def on_update(ex_id: str, symbol: str, ob: dict) -> None:
        raise AssertionError("on_update не должен вызываться в этом тесте")

    # RED: сейчас код НЕ использует wait_for и не логирует WS TIMEOUT.
    # В GREEN-версии мы завернём watch_order_book в asyncio.wait_for.
    monkeypatch.setattr("parsertang.streams.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("parsertang.streams.WS_WATCH_TIMEOUT_SECONDS", 0.01)

    with caplog.at_level(logging.INFO):
        task = asyncio.create_task(
            streams.subscribe_orderbooks({"gate": ["XRP/USDT"]}, on_update)
        )
        for _ in range(50):
            if "WS TIMEOUT | gate XRP/USDT" in caplog.text:
                break
            # Нужна реальная задержка, чтобы сработал asyncio.wait_for timeout.
            await real_sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert "WS WORKER START | gate symbol=XRP/USDT" in caplog.text
    assert "WS TIMEOUT | gate XRP/USDT" in caplog.text
