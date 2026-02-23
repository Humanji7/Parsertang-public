import pytest

from parsertang.ws_native.events import BBOEvent
from parsertang.ws_native.runner import NativeWsRunner


def test_runner_uses_router(monkeypatch):
    called = {"count": 0}

    def on_snapshot(_):
        called["count"] += 1

    runner = NativeWsRunner(on_snapshot=on_snapshot)
    runner._emit_snapshot(None)
    assert called["count"] == 1


@pytest.mark.asyncio
async def test_handle_event_emits_snapshot_when_depth_present():
    calls = {"count": 0}

    class DummyDepthCache:
        def refresh(self, ex_id, symbol, now=None):
            return None

        def get(self, ex_id, symbol, now=None):
            return {"depth": True}

    def build_snapshot(_ev, _depth, liquidity_window_pct, trade_volume_usd):
        assert liquidity_window_pct == 0.1
        assert trade_volume_usd == 100
        return {"snap": True}

    async def on_snapshot(_ex, _symbol, _snap):
        calls["count"] += 1

    runner = NativeWsRunner(
        on_snapshot=on_snapshot,
        depth_cache=DummyDepthCache(),
        build_snapshot=build_snapshot,
        liquidity_window_pct=0.1,
        trade_volume_usd=100,
    )
    ev = BBOEvent("bybit", "BTC/USDT", 100.0, 101.0, 0, 0)
    await runner.handle_event(ev)
    assert calls["count"] == 1
