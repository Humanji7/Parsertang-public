import asyncio
import time

from parsertang.config import settings
from parsertang.core.opportunity_evaluator import evaluate_arbitrage_for_symbol
from parsertang.core.state_manager import AppState, OrderbookSnapshot


def test_opportunity_evaluator_allows_side_specific_liquidity(monkeypatch):
    """
    Arbitrage needs:
      - buy exchange: enough ASK liquidity
      - sell exchange: enough BID liquidity

    It should not require both sides to meet liquidity threshold on each exchange,
    otherwise viable cross-exchange opportunities get dropped.
    """
    state = AppState()
    now = time.time()

    # High threshold to expose the bug
    monkeypatch.setattr(settings, "liquidity_usd_threshold", 5000.0, raising=False)
    monkeypatch.setattr(settings, "orderbook_stale_seconds", 9999.0, raising=False)
    monkeypatch.setattr(settings, "v2_truth_probe_enabled", False, raising=False)
    monkeypatch.setattr(settings, "v2_validation_enabled", False, raising=False)

    # buy exchange has enough ASK liquidity only
    state.orderbooks[("bybit", "AAA/USDT")] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1000.0,
        ask_liq_usd=6000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now,
    )

    # sell exchange has enough BID liquidity only
    state.orderbooks[("okx", "AAA/USDT")] = OrderbookSnapshot(
        best_bid=101.0,
        best_ask=102.0,
        bid_liq_usd=6000.0,
        ask_liq_usd=1000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now,
    )

    loop = asyncio.new_event_loop()
    try:
        evaluate_arbitrage_for_symbol("AAA/USDT", state, loop)
    finally:
        loop.close()

    # Without side-specific liquidity, evaluator would early-return before fee calc.
    # Since state.fee_manager is None, fee calc fails and increments arb_skip.
    assert state.funnel_counters.get("arb_skip", 0) == 1

