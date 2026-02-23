import asyncio
import time
from unittest.mock import patch

from parsertang.core.fee_calculator import FeeCalculationResult
from parsertang.core.opportunity_evaluator import evaluate_arbitrage_for_symbol
from parsertang.core.state_manager import AppState, OrderbookSnapshot
from parsertang.config import settings


def test_arbitrage_rejected_when_slippage_exceeds_budget() -> None:
    symbol = "AAA/USDT"
    now = time.time()

    # Create AppState instance
    state = AppState()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.15
    settings.slippage_budget_fraction = 0.25

    # Add orderbook snapshots
    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.10,
        ask_slip_pct=0.10,
        ts=now,
    )
    state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
        best_bid=100.5,
        best_ask=101.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.10,
        ask_slip_pct=0.10,
        ts=now,
    )

    # Pretend net profit passes threshold, but slippage budget should reject.
    # net_profit=0.16 => budget=0.04; slip_total=0.20 => reject.
    fake = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.16,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        with patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ):
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert state.funnel_counters["arb_reject_slippage"] == 1
    assert state.funnel_counters["arb_ok"] == 0


def test_arbitrage_passes_when_slippage_within_budget() -> None:
    symbol = "BBB/USDT"
    now = time.time()

    # Create AppState instance
    state = AppState()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.15
    settings.slippage_budget_fraction = 0.25

    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.01,
        ask_slip_pct=0.01,
        ts=now,
    )
    state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
        best_bid=100.5,
        best_ask=101.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.01,
        ask_slip_pct=0.01,
        ts=now,
    )

    # net_profit=0.20 => budget=0.05; slip_total=0.02 => ok.
    fake = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.20,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        with patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ):
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert state.funnel_counters["arb_ok"] == 1
    assert state.funnel_counters["slip_ok"] == 1
