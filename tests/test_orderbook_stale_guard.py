import asyncio
import sys
import time
import types
from unittest.mock import patch


def _install_telegram_stubs() -> None:
    telegram = types.ModuleType("telegram")
    telegram_constants = types.ModuleType("telegram.constants")
    telegram_ext = types.ModuleType("telegram.ext")

    class _Bot:  # minimal stub
        def __init__(self, *args, **kwargs) -> None:
            pass

    class _Application:  # minimal stub
        @classmethod
        def builder(cls):
            return cls()

        def token(self, *args, **kwargs):
            return self

        def build(self):
            return self

        def add_handler(self, *args, **kwargs):
            return None

        async def start(self):
            return None

        async def stop(self):
            return None

        async def initialize(self):
            return None

        async def shutdown(self):
            return None

    class _CommandHandler:  # minimal stub
        def __init__(self, *args, **kwargs) -> None:
            pass

    telegram.Bot = _Bot
    telegram_constants.ParseMode = types.SimpleNamespace(HTML="HTML")
    telegram_ext.Application = _Application
    telegram_ext.CommandHandler = _CommandHandler

    sys.modules.setdefault("telegram", telegram)
    sys.modules.setdefault("telegram.constants", telegram_constants)
    sys.modules.setdefault("telegram.ext", telegram_ext)


_install_telegram_stubs()
sys.modules.setdefault("ccxt", types.ModuleType("ccxt"))

from parsertang.config import settings  # noqa: E402
from parsertang.core.fee_calculator import FeeCalculationResult  # noqa: E402
from parsertang.core.opportunity_evaluator import (  # noqa: E402
    evaluate_arbitrage_for_symbol,
)
from parsertang.core.state_manager import AppState, OrderbookSnapshot  # noqa: E402


def test_stale_orderbook_skips_arbitrage() -> None:
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0

    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now - 10.0,
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
        net_profit_pct=0.2,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.new_event_loop()

    try:
        with patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ) as calc:
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert calc.call_count == 0
    assert state.funnel_counters["arb_skip_stale"] == 1


def test_fresh_orderbooks_call_fee_calc() -> None:
    symbol = "BBB/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0

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
        net_profit_pct=0.2,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.new_event_loop()

    try:
        with patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ) as calc:
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert calc.call_count == 1
