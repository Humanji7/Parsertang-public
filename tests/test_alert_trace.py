import asyncio
import sys
import time
import types


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
from parsertang.core.state_manager import AppState, OrderbookSnapshot  # noqa: E402
from parsertang.core import opportunity_evaluator as oe  # noqa: E402


def test_alert_trace_logs_insufficient_exchanges(caplog) -> None:
    symbol = "AAA/USDT"
    state = AppState()
    now = time.time()

    settings.alert_trace_enabled = True
    settings.alert_trace_symbols = symbol
    settings.alert_trace_interval_seconds = 0
    settings.orderbook_stale_seconds = 2.0

    oe._ALERT_TRACE_LAST_TS.clear()
    oe._ALERT_TRACE_SYMBOLS_RAW = None
    oe._ALERT_TRACE_SYMBOLS_SET = set()

    state.orderbooks[("bybit", symbol)] = OrderbookSnapshot(
        best_bid=99.0,
        best_ask=100.0,
        bid_liq_usd=1_000_000.0,
        ask_liq_usd=1_000_000.0,
        bid_slip_pct=0.0,
        ask_slip_pct=0.0,
        ts=now,
    )

    loop = asyncio.get_event_loop()

    with caplog.at_level("INFO"):
        oe.evaluate_arbitrage_for_symbol(symbol, state, loop)

    assert f"ARB TRACE | {symbol} reason=insufficient_exchanges" in caplog.text
