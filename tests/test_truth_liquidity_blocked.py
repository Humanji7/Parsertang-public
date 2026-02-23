import asyncio
import logging
import time
from unittest.mock import patch

from parsertang.config import settings
from parsertang.core.fee_calculator import FeeCalculationResult
from parsertang.core.opportunity_evaluator import evaluate_arbitrage_for_symbol
from parsertang.core.state_manager import AppState, OrderbookSnapshot
from parsertang.v2.validator import ValidationResult


def test_truth_counts_rest_liquidity_as_ok_blocked(caplog) -> None:
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.use_dynamic_withdrawal_fees = False

    state.alert_service = None
    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now,
    )
    state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
        best_bid=101.0,
        best_ask=102.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now,
    )

    fake = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.5,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.get_event_loop()
    bad = ValidationResult(ok=False, reason="rest_ask_liq", rest_buy=100.0, rest_sell=101.0)

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch("parsertang.core.opportunity_evaluator.Validator.validate", return_value=bad),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any(
        rec.message.startswith("TRUTH OK |")
        and symbol in rec.message
        and "reason=rest_ask_liq_blocked" in rec.message
        for rec in caplog.records
    )
    assert not any(
        rec.message.startswith("TRUTH FAIL |") and symbol in rec.message
        for rec in caplog.records
    )
