import logging

import pytest

from parsertang.streams import Streams


@pytest.mark.asyncio
async def test_ws_markets_failed_logs_time_skew_for_bybit(caplog):
    class DummyWsExchange:
        def load_markets(self):
            raise Exception(
                'bybit {"retCode":10002,"retMsg":"invalid request: '
                'req_timestamp[1000],server_timestamp[2500],recv_window[5000]"}'
            )

    streams = Streams.__new__(Streams)

    with caplog.at_level(logging.WARNING):
        ok = await streams._ensure_markets_loaded("bybit", DummyWsExchange())

    assert ok is False
    assert "WS MARKETS FAILED | bybit" in caplog.text
    assert "WS MARKETS TIME SKEW | bybit" in caplog.text
    assert "diff_ms=1500" in caplog.text
