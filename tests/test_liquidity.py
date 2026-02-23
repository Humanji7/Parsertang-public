import pytest

from parsertang.liquidity import liquidity_usd_within_window


def test_basic_window():
    bids = [(100.0, 1.0), (99.9, 5.0)]
    asks = [(100.2, 1.0), (100.3, 5.0)]
    bid_usd, ask_usd = liquidity_usd_within_window(bids, asks, 0.1)
    assert bid_usd == pytest.approx(100.0)
    assert ask_usd == pytest.approx(100.2)


def test_empty_books():
    bid_usd, ask_usd = liquidity_usd_within_window([], [], 0.1)
    assert bid_usd == 0.0
    assert ask_usd == 0.0


def test_outside_window():
    bids = [(98.0, 10.0)]
    asks = [(102.0, 10.0)]
    bid_usd, ask_usd = liquidity_usd_within_window(bids, asks, 0.1)
    assert bid_usd == 0.0
    assert ask_usd == 0.0


def test_ignores_extra_depth_fields():
    bids = [(100.0, 1.0, 5), (99.9, 2.0, 3)]
    asks = [(100.2, 1.5, 7), (100.3, 1.0, 4)]
    bid_usd, ask_usd = liquidity_usd_within_window(bids, asks, 0.1)
    assert bid_usd == pytest.approx(100.0)
    assert ask_usd == pytest.approx(150.3)
