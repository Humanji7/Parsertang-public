from parsertang.v2.health_metrics import compute_health_snapshot
from parsertang.v2.models import Event


def test_compute_health_snapshot():
    now_ms = 10_000.0
    events = {
        ("bybit", "AAA/USDT"): Event("bybit", "orderbook", "AAA/USDT", 0, 9_000, {}),
        ("okx", "AAA/USDT"): Event("okx", "orderbook", "AAA/USDT", 0, 9_500, {}),
    }
    snap = compute_health_snapshot(
        events,
        now_ms=now_ms,
        max_age_ms=2_000,
        expected_exchanges=["bybit", "okx", "mexc"],
    )
    assert snap.fresh_ratio_by_ex["bybit"] == 1.0
    assert snap.fresh_ratio_by_ex["okx"] == 1.0
    assert snap.fresh_ratio_by_ex["mexc"] == 0.0
    assert set(snap.fresh_ratio_by_ex.keys()) == {"bybit", "okx", "mexc"}
    assert snap.fresh_ratio_min == 0.0
