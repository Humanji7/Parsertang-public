"""Core modules for Parsertang arbitrage system.

Centralized state management, fee calculation, opportunity evaluation,
orderbook processing, and metrics logging.
"""

from parsertang.core.fee_calculator import (
    FeeCalculationResult,
    calculate_opportunity_fees_and_network,
)
from parsertang.core.metrics_logger import (
    cleanup_stale_data,
    log_liquidity_summary,
    log_memory_health,
    log_spread_distribution,
    log_trader_stats,
    log_ws_health,
    start_metrics_logger,
)
_opportunity_exports = []
try:  # Optional in minimal test environments (telegram dependency)
    from parsertang.core.opportunity_evaluator import (  # type: ignore
        ALERT_HARD_COOLDOWN_SECONDS,
        ALERT_SOFT_COOLDOWN_SECONDS,
        ARB_OK_LOG_COOLDOWN_SECONDS,
        evaluate_arbitrage_for_symbol,
    )

    _opportunity_exports = [
        "ALERT_HARD_COOLDOWN_SECONDS",
        "ALERT_SOFT_COOLDOWN_SECONDS",
        "ARB_OK_LOG_COOLDOWN_SECONDS",
        "evaluate_arbitrage_for_symbol",
    ]
except ModuleNotFoundError:
    ALERT_HARD_COOLDOWN_SECONDS = None  # type: ignore[assignment]
    ALERT_SOFT_COOLDOWN_SECONDS = None  # type: ignore[assignment]
    ARB_OK_LOG_COOLDOWN_SECONDS = None  # type: ignore[assignment]
    evaluate_arbitrage_for_symbol = None  # type: ignore[assignment]
from parsertang.core.orderbook_processor import (
    on_orderbook_update,
    on_orderbook_update_async,
    parse_orderbook_entries,
)
from parsertang.core.state_manager import AppState, OrderbookSnapshot, WSMetrics

__all__ = [
    "AppState",
    "OrderbookSnapshot",
    "WSMetrics",
    "FeeCalculationResult",
    "calculate_opportunity_fees_and_network",
    *_opportunity_exports,
    "on_orderbook_update",
    "on_orderbook_update_async",
    "parse_orderbook_entries",
    "log_spread_distribution",
    "log_ws_health",
    "log_liquidity_summary",
    "log_trader_stats",
    "log_memory_health",
    "cleanup_stale_data",
    "start_metrics_logger",
]
