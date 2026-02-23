from parsertang.slippage import (
    compute_vwap_buy_for_usd,
    compute_vwap_sell_for_base,
    estimate_buy_slippage_pct,
    estimate_sell_slippage_pct,
)


def test_compute_vwap_buy_for_usd_fills_exact() -> None:
    asks = [(100.0, 1.0), (101.0, 1.0)]
    vwap = compute_vwap_buy_for_usd(asks, target_usd=150.0)
    assert vwap is not None
    # Spend 100 at 100 + 50 at 101 => base=1 + 0.4950495..., VWAP ~ 100.334
    assert 100.2 < vwap < 100.5


def test_compute_vwap_buy_for_usd_returns_none_when_insufficient() -> None:
    asks = [(100.0, 1.0)]
    assert compute_vwap_buy_for_usd(asks, target_usd=200.0) is None


def test_compute_vwap_sell_for_base_fills_exact() -> None:
    bids = [(100.0, 1.0), (99.0, 1.0)]
    vwap = compute_vwap_sell_for_base(bids, target_base=1.5)
    assert vwap is not None
    # Sell 1 @100 + 0.5 @99 => vwap = (100 + 49.5) / 1.5 = 99.666...
    assert 99.5 < vwap < 99.9


def test_estimate_buy_slippage_pct_is_positive() -> None:
    asks = [(100.0, 1.0), (101.0, 1.0)]
    slip = estimate_buy_slippage_pct(asks, target_usd=150.0)
    assert 0.0 < slip < 1.0


def test_estimate_sell_slippage_pct_is_positive() -> None:
    bids = [(100.0, 1.0), (99.0, 1.0)]
    slip = estimate_sell_slippage_pct(bids, target_usd=150.0)
    assert 0.0 < slip < 1.0
