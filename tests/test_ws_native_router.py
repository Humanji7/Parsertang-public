import time

from parsertang.ws_native.router import build_snapshot
from parsertang.ws_native.events import BBOEvent
from parsertang.ws_native.depth_cache import DepthSnapshot


def test_build_snapshot_uses_bbo_and_depth():
    ev = BBOEvent("bybit", "BTC/USDT", 100.0, 101.0, 0, 0)
    depth = DepthSnapshot(bids=[[100.0, 1.0]], asks=[[101.0, 1.0]], ts=time.time())
    snap = build_snapshot(ev, depth, liquidity_window_pct=0.1, trade_volume_usd=100)
    assert snap.best_bid == 100.0
    assert snap.best_ask == 101.0
