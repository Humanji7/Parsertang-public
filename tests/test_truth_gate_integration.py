import asyncio
import json
import sys
import time
import types
from contextlib import ExitStack
from unittest.mock import patch


def _install_telegram_stubs() -> None:
    telegram = types.ModuleType("telegram")
    telegram_constants = types.ModuleType("telegram.constants")
    telegram_ext = types.ModuleType("telegram.ext")

    class _Bot:
        def __init__(self, *args, **kwargs) -> None:
            pass

    class _Application:
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

    class _CommandHandler:
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
from parsertang.core.opportunity_evaluator import evaluate_arbitrage_for_symbol  # noqa: E402
from parsertang.core.state_manager import AppState, OrderbookSnapshot  # noqa: E402


class DummyAlertService:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.tech: list[str] = []

    def send(self, text: str) -> None:
        self.sent.append(text)

    async def send_tech(self, text: str) -> None:
        self.tech.append(text)


def test_truth_gate_blocks_alert_when_ratio_low(tmp_path):
    symbol = "AAA/USDT"
    now = time.time()
    state = AppState()
    state.alert_service = DummyAlertService()

    summary = {"ok": 90, "fail": 10, "ratio": 90.0}
    summary_path = tmp_path / "truth_summary.json"
    summary_path.write_text(json.dumps(summary))

    with ExitStack() as stack:
        stack.enter_context(patch.object(settings, "truth_gate_enabled", True))
        stack.enter_context(patch.object(settings, "truth_gate_ratio_min", 98.0))
        stack.enter_context(
            patch.object(settings, "truth_gate_summary_path", str(summary_path))
        )
        stack.enter_context(patch.object(settings, "truth_gate_max_age_seconds", 3600))
        stack.enter_context(patch.object(settings, "truth_gate_refresh_seconds", 0.0))
        stack.enter_context(patch.object(settings, "liquidity_usd_threshold", 0.0))
        stack.enter_context(patch.object(settings, "min_net_profit", 0.0))
        stack.enter_context(patch.object(settings, "orderbook_stale_seconds", 10.0))
        stack.enter_context(patch.object(settings, "v2_validation_enabled", False))
        stack.enter_context(patch.object(settings, "use_dynamic_withdrawal_fees", False))

        state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
            best_bid=101.0,
            best_ask=102.0,
            bid_liq_usd=1_000_000.0,
            ask_liq_usd=1_000_000.0,
            bid_slip_pct=0.0,
            ask_slip_pct=0.0,
            ts=now,
        )
        state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
            best_bid=103.0,
            best_ask=104.0,
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

        loop = asyncio.new_event_loop()
        try:
            with patch(
                "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
                return_value=fake,
            ):
                evaluate_arbitrage_for_symbol(symbol, state, loop)

            loop.run_until_complete(asyncio.sleep(0))
        finally:
            loop.close()

        assert state.alert_service.sent == []
        assert state.alert_service.tech != []


def test_truth_allowlist_blocks_alert_when_symbol_not_green(tmp_path):
    symbol = "AAA/USDT"
    now = time.time()
    state = AppState()
    state.alert_service = DummyAlertService()

    summary = {"ok": 980, "fail": 20, "ratio": 98.0}
    summary_path = tmp_path / "truth_summary.json"
    summary_path.write_text(json.dumps(summary))

    allowlist_path = tmp_path / "truth_allowlist.json"
    allowlist_path.write_text(json.dumps({"symbols": ["BBB/USDT"]}))

    with ExitStack() as stack:
        stack.enter_context(patch.object(settings, "truth_gate_enabled", True))
        stack.enter_context(patch.object(settings, "truth_gate_ratio_min", 98.0))
        stack.enter_context(patch.object(settings, "truth_gate_min_total", 10))
        stack.enter_context(
            patch.object(settings, "truth_gate_summary_path", str(summary_path))
        )
        stack.enter_context(patch.object(settings, "truth_gate_max_age_seconds", 3600))
        stack.enter_context(patch.object(settings, "truth_gate_refresh_seconds", 0.0))
        stack.enter_context(patch.object(settings, "truth_allowlist_path", str(allowlist_path)))
        stack.enter_context(patch.object(settings, "truth_allowlist_refresh_seconds", 0.0))
        stack.enter_context(patch.object(settings, "liquidity_usd_threshold", 0.0))
        stack.enter_context(patch.object(settings, "min_net_profit", 0.0))
        stack.enter_context(patch.object(settings, "orderbook_stale_seconds", 10.0))
        stack.enter_context(patch.object(settings, "v2_validation_enabled", False))
        stack.enter_context(patch.object(settings, "use_dynamic_withdrawal_fees", False))

        state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
            best_bid=101.0,
            best_ask=102.0,
            bid_liq_usd=1_000_000.0,
            ask_liq_usd=1_000_000.0,
            bid_slip_pct=0.0,
            ask_slip_pct=0.0,
            ts=now,
        )
        state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
            best_bid=103.0,
            best_ask=104.0,
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

        loop = asyncio.new_event_loop()
        try:
            with patch(
                "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
                return_value=fake,
            ):
                evaluate_arbitrage_for_symbol(symbol, state, loop)

            loop.run_until_complete(asyncio.sleep(0))
        finally:
            loop.close()

        assert state.alert_service.sent == []
        assert state.alert_service.tech != []
