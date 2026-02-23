import asyncio
import logging
import sys
import time
import types
from unittest.mock import MagicMock, patch


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
from parsertang.v2.validator import ValidationResult  # noqa: E402
from parsertang.withdrawal_fees import WithdrawalFeeCache  # noqa: E402


class DummyAlertService:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send(self, text: str) -> None:
        self.sent.append(text)


class DummyTechAlertService:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_tech(self, text: str) -> None:
        self.sent.append(text)


def test_profit_mode_funded_allows_alert_even_if_transfer_net_below_threshold():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    orig = {
        "liquidity_usd_threshold": settings.liquidity_usd_threshold,
        "min_net_profit": settings.min_net_profit,
        "orderbook_stale_seconds": settings.orderbook_stale_seconds,
        "v2_validation_enabled": settings.v2_validation_enabled,
        "use_dynamic_withdrawal_fees": settings.use_dynamic_withdrawal_fees,
        "truth_gate_enabled": settings.truth_gate_enabled,
    }

    try:
        settings.liquidity_usd_threshold = 0.0
        settings.min_net_profit = 0.15
        settings.orderbook_stale_seconds = 10.0
        settings.v2_validation_enabled = True
        settings.use_dynamic_withdrawal_fees = False
        settings.truth_gate_enabled = False
        settings.profit_mode = "funded"

        state.alert_service = DummyAlertService()

        state.orderbooks[("mexc", symbol)] = OrderbookSnapshot(
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
            network="SOL",
            withdraw_fee_base=1.0,
            buy_fee_pct=0.05,
            sell_fee_pct=0.05,
            withdraw_fee_pct=0.30,
            net_profit_pct=0.10,  # transfer net (below threshold)
            net_profit_funded_pct=0.20,  # funded net (above threshold)
            error_reason=None,
            fee_confidence="HIGH",
        )

        loop = asyncio.get_event_loop()

        with (
            patch(
                "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
                return_value=fake,
            ),
            patch(
                "parsertang.core.opportunity_evaluator.Validator.validate",
                return_value=ValidationResult(
                    ok=True, reason="ok", rest_buy=100.0, rest_sell=101.0
                ),
            ),
        ):
            evaluate_arbitrage_for_symbol(symbol, state, loop)

        assert len(state.alert_service.sent) == 1
    finally:
        for k, v in orig.items():
            setattr(settings, k, v)
        settings.profit_mode = "transfer"


def test_v2_validation_blocks_alert():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.use_dynamic_withdrawal_fees = False
    settings.use_dynamic_withdrawal_fees = False
    settings.use_dynamic_withdrawal_fees = False
    settings.use_dynamic_withdrawal_fees = False

    state.alert_service = DummyAlertService()

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

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(ok=False, reason="rest_buy_price"),
        ),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert state.alert_service.sent == []
    assert state.funnel_counters["alerts_invalid"] == 1


def test_v2_validation_blacklists_ws_stale_symbol():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time() - 2.0

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.v2_validation_ws_max_age_ms = 500
    settings.v2_validation_ws_max_skew_ms = 500
    settings.v2_validation_stale_symbol_threshold = 2
    settings.v2_validation_stale_symbol_cooldown_seconds = 60
    settings.use_dynamic_withdrawal_fees = False

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

    with patch(
        "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
        return_value=fake,
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert state.validation_symbol_blacklist.get(symbol) is None
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert symbol in state.validation_symbol_blacklist
        previous_invalid = state.funnel_counters["alerts_invalid"]
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert state.funnel_counters["alerts_invalid"] == previous_invalid

    settings.v2_validation_stale_symbol_threshold = 5
    settings.v2_validation_stale_symbol_cooldown_seconds = 600
    settings.v2_validation_rest_symbol_threshold = 10
    settings.v2_validation_rest_symbol_cooldown_seconds = 1800
    settings.use_dynamic_withdrawal_fees = True


def test_v2_validation_blacklists_rest_divergence_symbol():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.v2_validation_ws_max_age_ms = 5_000
    settings.v2_validation_ws_max_skew_ms = 5_000
    settings.v2_validation_stale_symbol_threshold = 0
    settings.v2_validation_stale_symbol_cooldown_seconds = 0
    settings.v2_validation_rest_symbol_threshold = 2
    settings.v2_validation_rest_symbol_cooldown_seconds = 60
    settings.use_dynamic_withdrawal_fees = False

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
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert state.validation_symbol_blacklist.get(symbol) is None
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert symbol in state.validation_symbol_blacklist
        previous_invalid = state.funnel_counters["alerts_invalid"]
        evaluate_arbitrage_for_symbol(symbol, state, loop)
        assert state.funnel_counters["alerts_invalid"] == previous_invalid

    settings.v2_validation_stale_symbol_threshold = 5
    settings.v2_validation_stale_symbol_cooldown_seconds = 600
    settings.v2_validation_rest_symbol_threshold = 10
    settings.v2_validation_rest_symbol_cooldown_seconds = 1800
    settings.use_dynamic_withdrawal_fees = True


def test_v2_validation_runs_without_alert_service():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True

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

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(ok=False, reason="rest_buy_price"),
        ),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert state.funnel_counters["alerts_invalid"] == 1
    settings.use_dynamic_withdrawal_fees = True


def test_v2_validation_ws_stale_measured_at_selection_time():
    symbol = "AAA/USDT"
    state = AppState()

    t = {"now": time.time()}

    def fake_time() -> float:
        return float(t["now"])

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.v2_validation_ws_max_age_ms = 3000
    settings.v2_validation_ws_max_skew_ms = 1500
    settings.use_dynamic_withdrawal_fees = False
    settings.truth_gate_enabled = False

    state.alert_service = DummyAlertService()

    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=t["now"],
    )
    state.orderbooks[("okx", symbol)] = OrderbookSnapshot(
        best_bid=101.0,
        best_ask=102.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=t["now"],
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

    def fake_fee_calc(*args, **kwargs):
        t["now"] += 4.0
        return fake

    with (
        patch("parsertang.core.opportunity_evaluator.time.time", side_effect=fake_time),
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            side_effect=fake_fee_calc,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(ok=True, reason="ok"),
        ),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert state.funnel_counters["alerts_invalid"] == 0


def test_truth_log_emitted_on_validation_fail(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.use_dynamic_withdrawal_fees = False

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

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=False, reason="rest_buy_price", rest_buy=101.0, rest_sell=99.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH FAIL" in rec.message for rec in caplog.records)
    assert any("rest_buy_price" in rec.message for rec in caplog.records)
    settings.use_dynamic_withdrawal_fees = True


def test_truth_log_emitted_on_fee_validation_ok(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
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

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=True, reason="ok", rest_buy=100.0, rest_sell=101.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH OK" in rec.message for rec in caplog.records)
    settings.use_dynamic_withdrawal_fees = True


def test_truth_probe_runs_when_alert_suppressed(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.v2_truth_probe_enabled = True
    settings.v2_truth_probe_interval_seconds = 0.0
    settings.use_dynamic_withdrawal_fees = False

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

    alert_key = (symbol, "bybit", "okx")
    state.last_alert_ts[alert_key] = (time.monotonic(), 0.4)

    loop = asyncio.get_event_loop()

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=False, reason="rest_buy_price", rest_buy=101.0, rest_sell=99.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH FAIL" in rec.message for rec in caplog.records)

    settings.v2_truth_probe_enabled = False
    settings.v2_truth_probe_interval_seconds = 30.0
    settings.use_dynamic_withdrawal_fees = True


def test_truth_probe_runs_below_threshold(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 1.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.v2_truth_probe_enabled = True
    settings.v2_truth_probe_interval_seconds = 0.0
    settings.use_dynamic_withdrawal_fees = False

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

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=False, reason="rest_buy_price", rest_buy=101.0, rest_sell=99.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH FAIL" in rec.message for rec in caplog.records)

    settings.v2_truth_probe_enabled = False
    settings.v2_truth_probe_interval_seconds = 30.0
    settings.use_dynamic_withdrawal_fees = True


def test_truth_probe_promotes_profitable_rest_to_alert(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.15
    settings.orderbook_stale_seconds = 999.0
    settings.v2_validation_enabled = True
    settings.v2_truth_probe_enabled = True
    settings.v2_truth_probe_interval_seconds = 0.0
    settings.use_dynamic_withdrawal_fees = False
    settings.truth_gate_enabled = False
    settings.truth_allowlist_path = None

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

    # First call: WS-based fee calc below threshold.
    fake_ws = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.10,
        error_reason=None,
        fee_confidence="HIGH",
    )
    # Second call: REST-based fee calc is profitable.
    fake_rest = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.20,
        error_reason=None,
        fee_confidence="HIGH",
    )

    state.alert_service = MagicMock()
    loop = asyncio.get_event_loop()

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            side_effect=[fake_ws, fake_rest],
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=True, reason="ok", rest_buy=100.0, rest_sell=101.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert state.alert_service.send.call_count == 1
    assert any("TRUTH PROBE PROMOTE" in rec.message for rec in caplog.records)
    assert any("ALERT SENT |" in rec.message for rec in caplog.records)

    settings.v2_truth_probe_enabled = False
    settings.v2_truth_probe_interval_seconds = 30.0
    settings.use_dynamic_withdrawal_fees = True
    settings.truth_gate_enabled = True


def test_truth_below_threshold_counts_ok_when_consistent(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 1.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.v2_truth_probe_enabled = True
    settings.v2_truth_probe_interval_seconds = 0.0
    settings.use_dynamic_withdrawal_fees = False

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

    # First call: WS-based fee calc (below threshold)
    fake_ws = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.5,
        error_reason=None,
        fee_confidence="HIGH",
    )
    # Second call: REST-based fee calc (also below threshold)
    fake_rest = FeeCalculationResult(
        network="TRC20",
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.4,
        error_reason=None,
        fee_confidence="HIGH",
    )

    loop = asyncio.get_event_loop()

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            side_effect=[fake_ws, fake_rest],
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=True, reason="ok", rest_buy=100.0, rest_sell=101.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH OK" in rec.message for rec in caplog.records)
    assert not any("TRUTH FAIL" in rec.message for rec in caplog.records)


def test_truth_probe_runs_when_fee_calc_invalid(caplog):
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.v2_truth_probe_enabled = True
    settings.v2_truth_probe_interval_seconds = 0.0
    settings.use_dynamic_withdrawal_fees = False

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

    fake_invalid = FeeCalculationResult(
        network=None,
        withdraw_fee_base=0.0,
        buy_fee_pct=0.0,
        sell_fee_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=0.0,
        error_reason="no_fee_data",
        fee_confidence="LOW",
    )

    loop = asyncio.get_event_loop()

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake_invalid,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=True, reason="ok", rest_buy=100.0, rest_sell=101.0
            ),
        ),
        caplog.at_level(logging.INFO),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert any("TRUTH PROBE" in rec.message for rec in caplog.records)

    settings.v2_truth_probe_enabled = False
    settings.v2_truth_probe_interval_seconds = 30.0
    settings.use_dynamic_withdrawal_fees = True


def test_truth_fail_sends_tech_alert():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 2.0
    settings.v2_validation_enabled = True
    settings.use_dynamic_withdrawal_fees = False
    settings.v2_truth_fail_tech_alert_enabled = True
    settings.v2_truth_fail_tech_alert_interval_seconds = 0.0

    state.alert_service = DummyTechAlertService()

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

    created: list[asyncio.Future] = []

    class DummyLoop:
        def create_task(self, coro):
            created.append(coro)
            return coro

    loop = DummyLoop()

    with (
        patch(
            "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
            return_value=fake,
        ),
        patch(
            "parsertang.core.opportunity_evaluator.Validator.validate",
            return_value=ValidationResult(
                ok=False, reason="rest_buy_price", rest_buy=101.0, rest_sell=99.0
            ),
        ),
    ):
        evaluate_arbitrage_for_symbol(symbol, state, loop)  # type: ignore[arg-type]

    assert created, "expected send_tech task to be scheduled"
    asyncio.run(created[0])
    assert any("TRUTH FAIL" in msg for msg in state.alert_service.sent)

    settings.v2_truth_fail_tech_alert_enabled = False
    settings.v2_truth_fail_tech_alert_interval_seconds = 300.0


def test_v2_validation_blocks_when_ws_stale():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time() - 2.0

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.v2_validation_ws_max_age_ms = 500
    settings.use_dynamic_withdrawal_fees = False

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

    loop = asyncio.new_event_loop()

    try:
        with (
            patch(
                "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
                return_value=fake,
            ),
            patch(
                "parsertang.core.opportunity_evaluator.Validator.validate",
                side_effect=AssertionError("REST validator should not run"),
            ),
        ):
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert state.funnel_counters["alerts_invalid"] == 1
    settings.v2_validation_fee_max_age_seconds = 3600


def test_v2_validation_blocks_when_fee_cache_stale():
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.liquidity_usd_threshold = 0.0
    settings.min_net_profit = 0.0
    settings.orderbook_stale_seconds = 10.0
    settings.v2_validation_enabled = True
    settings.v2_validation_ws_max_age_ms = 10_000
    settings.v2_validation_ws_max_skew_ms = 10_000
    settings.v2_validation_fee_max_age_seconds = 60
    settings.use_dynamic_withdrawal_fees = True

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

    cache = WithdrawalFeeCache(
        fees={},
        last_updated=now - 120,
        cache_lifetime=3600,
    )
    state.fee_manager = types.SimpleNamespace(cache=cache)

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
        with (
            patch(
                "parsertang.core.opportunity_evaluator.calculate_opportunity_fees_and_network",
                return_value=fake,
            ),
            patch(
                "parsertang.core.opportunity_evaluator.Validator.validate",
                side_effect=AssertionError("REST validator should not run"),
            ),
        ):
            evaluate_arbitrage_for_symbol(symbol, state, loop)
    finally:
        loop.close()

    assert state.funnel_counters["alerts_invalid"] == 1
