"""Metrics logging and background monitoring tasks.

Provides periodic metric logging for:
- Arbitrage opportunity funnel (detection efficiency)
- Spread distribution (profitability analysis)
- WebSocket health (connectivity monitoring)
- Liquidity summary (exchange coverage)
- Trader statistics (cycle success/failure)
- Memory health (state cleanup tracking)

All metric functions accept AppState to avoid global state.
Background tasks run independently and reset counters after logging.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import TYPE_CHECKING

from parsertang.config import settings

if TYPE_CHECKING:
    from parsertang.core.state_manager import AppState

logger = logging.getLogger(__name__)


def format_validation_summary(*, valid: int, invalid: int) -> str:
    total = valid + invalid
    ratio = (valid / total * 100.0) if total else 0.0
    return f"V2 VALIDATION SUMMARY | valid={valid} invalid={invalid} ratio={ratio:.1f}%"


def format_fee_validation_summary(
    *,
    ok: int,
    fail: int,
    reasons: dict[str, int],
    samples: dict[str, tuple[str, str, str]],
) -> str:
    total = ok + fail
    ratio = (ok / total * 100.0) if total else 0.0
    top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:3]
    parts = []
    for reason, count in top:
        sample = samples.get(reason)
        sample_str = ""
        if sample:
            sample_str = f" sample={sample[0]} {sample[1]}→{sample[2]}"
        parts.append(f"{reason}={count}{sample_str}")
    detail = " ".join(parts) if parts else "-"
    return (
        f"FEE VALIDATION SUMMARY | ok={ok} fail={fail} ratio={ratio:.1f}% reasons={detail}"
    )


def format_truth_probe_summary(*, ok: int, fail: int) -> str:
    total = ok + fail
    ratio = (ok / total * 100.0) if total else 0.0
    return f"TRUTH SUMMARY | ok={ok} fail={fail} ratio={ratio:.1f}%"


def format_alert_funnel_summary(
    *,
    candidates: int,
    sent: int,
    error: int,
    truth_gate_blocked: int,
    truth_allowlist_blocked: int,
    fee_live_blocked: int,
    min_repeat: int,
    hard_cooldown: int,
    soft_cooldown: int,
) -> str:
    return (
        "ALERT FUNNEL | "
        f"candidates={candidates} sent={sent} error={error} "
        f"blocked(truth_gate={truth_gate_blocked} truth_allowlist={truth_allowlist_blocked} fee_live={fee_live_blocked}) "
        f"dedup(min_repeat={min_repeat} hard={hard_cooldown} soft={soft_cooldown})"
    )


async def log_validation_summary(state: AppState) -> None:
    """Periodically log V2 validation summary metrics."""
    while True:
        await asyncio.sleep(60)

        async with state.metrics_lock:
            valid = state.funnel_counters.pop("alerts_valid", 0)
            invalid = state.funnel_counters.pop("alerts_invalid", 0)

        logger.info(format_validation_summary(valid=valid, invalid=invalid))


async def log_fee_validation_summary(state: AppState) -> None:
    """Periodically log fee validation summary metrics."""
    while True:
        await asyncio.sleep(60)

        async with state.metrics_lock:
            ok = state.funnel_counters.pop("fee_validation_ok", 0)
            fail = state.funnel_counters.pop("fee_validation_fail", 0)
            reasons = dict(state.fee_validation_reasons)
            samples = dict(state.fee_validation_samples)
            state.fee_validation_reasons.clear()
            state.fee_validation_samples.clear()

        if ok or fail:
            logger.info(
                format_fee_validation_summary(
                    ok=ok, fail=fail, reasons=reasons, samples=samples
                )
            )


async def log_truth_probe_summary(state: AppState) -> None:
    """Periodically log truth-probe summary metrics."""
    while True:
        interval = float(max(settings.v2_truth_probe_summary_interval_seconds, 0.0))
        if interval <= 0:
            interval = 60.0
        await asyncio.sleep(interval)

        async with state.metrics_lock:
            ok = state.funnel_counters.pop("truth_ok", 0)
            fail = state.funnel_counters.pop("truth_fail", 0)

        text = format_truth_probe_summary(ok=ok, fail=fail)
        logger.info(text)
        if settings.v2_truth_probe_tech_summary_enabled and state.alert_service:
            await state.alert_service.send_tech(text)


async def log_alert_funnel_summary(state: AppState) -> None:
    """Periodically log alert funnel metrics (candidate → suppressed → sent)."""
    while True:
        await asyncio.sleep(60)

        async with state.metrics_lock:
            candidates = state.funnel_counters.pop("alerts_candidate", 0)
            sent = state.funnel_counters.pop("alerts_sent", 0)
            error = state.funnel_counters.pop("alerts_error", 0)
            truth_gate_blocked = state.funnel_counters.pop("alerts_truth_gate_blocked", 0)
            truth_allowlist_blocked = state.funnel_counters.pop(
                "alerts_truth_allowlist_blocked", 0
            )
            fee_live_blocked = state.funnel_counters.pop("alerts_fee_live_blocked", 0)
            min_repeat = state.funnel_counters.pop("alerts_min_repeat", 0)
            hard_cooldown = state.funnel_counters.pop("alerts_hard_cooldown", 0)
            soft_cooldown = state.funnel_counters.pop("alerts_soft_cooldown", 0)

        if (
            candidates
            or sent
            or error
            or truth_gate_blocked
            or truth_allowlist_blocked
            or fee_live_blocked
            or min_repeat
            or hard_cooldown
            or soft_cooldown
        ):
            logger.info(
                format_alert_funnel_summary(
                    candidates=candidates,
                    sent=sent,
                    error=error,
                    truth_gate_blocked=truth_gate_blocked,
                    truth_allowlist_blocked=truth_allowlist_blocked,
                    fee_live_blocked=fee_live_blocked,
                    min_repeat=min_repeat,
                    hard_cooldown=hard_cooldown,
                    soft_cooldown=soft_cooldown,
                )
            )


def _format_ws_exchange_stat(
    *,
    ex_id: str,
    updates: int,
    unique_symbols: int,
    allocated: int,
    stale_intervals: int,
) -> str:
    """Format WebSocket exchange statistics for logging.

    Args:
        ex_id: Exchange ID (e.g., "bybit")
        updates: Number of orderbook updates received
        unique_symbols: Number of unique symbols seen
        allocated: Expected number of symbols allocated to this exchange
        stale_intervals: Consecutive intervals with 0 updates

    Returns:
        Formatted stat string: "bybit=123/45sym alloc=50 stale=0"
    """
    return (
        f"{ex_id}={updates}/{unique_symbols}sym "
        f"alloc={allocated} stale={stale_intervals}"
    )


def _update_ws_stale_intervals(
    stale: defaultdict[str, int],
    *,
    ex_id: str,
    allocated: int,
    updates: int,
) -> None:
    """Update stale interval counter for exchange.

    Increments stale counter if allocated symbols exist but no updates received.
    Resets counter to 0 when updates are received or no symbols allocated.

    Args:
        stale: Mutable stale interval counter dict
        ex_id: Exchange ID
        allocated: Number of symbols allocated to exchange
        updates: Number of updates received this interval
    """
    if allocated <= 0:
        stale[ex_id] = 0
        return
    if updates <= 0:
        stale[ex_id] += 1
    else:
        stale[ex_id] = 0


async def log_spread_distribution(state: AppState) -> None:
    """Periodically log SPREAD DISTRIBUTION to analyze profitability patterns.

    Logs every 5 minutes with spread buckets showing distribution of gross spreads:
    - negative: Inverted spread (bid < ask across exchanges)
    - 0-0.1%: Minimal spread
    - 0.1-0.3%: Low spread
    - 0.3-0.5%: Medium spread
    - >0.5%: High spread (potentially profitable)

    This helps understand market conditions and calibrate MIN_NET_PROFIT threshold.

    Args:
        state: AppState instance containing spread_buckets
    """
    while True:
        await asyncio.sleep(300)  # Log every 5 minutes

        async with state.metrics_lock:
            now = time.time()
            if now - state.spread_last_logged < 300:
                continue

            if not state.spread_buckets:
                state.spread_last_logged = now
                continue

            total = sum(state.spread_buckets.values())
            if total == 0:
                state.spread_last_logged = now
                continue

            logger.info(
                "SPREAD DIST | negative=%d (%.1f%%) 0-0.1=%d (%.1f%%) 0.1-0.3=%d (%.1f%%) "
                "0.3-0.5=%d (%.1f%%) >0.5=%d (%.1f%%) total=%d",
                state.spread_buckets["negative"],
                state.spread_buckets["negative"] / total * 100,
                state.spread_buckets["0-0.1"],
                state.spread_buckets["0-0.1"] / total * 100,
                state.spread_buckets["0.1-0.3"],
                state.spread_buckets["0.1-0.3"] / total * 100,
                state.spread_buckets["0.3-0.5"],
                state.spread_buckets["0.3-0.5"] / total * 100,
                state.spread_buckets[">0.5"],
                state.spread_buckets[">0.5"] / total * 100,
                total,
            )

            # Reset buckets for next interval
            state.spread_buckets.clear()
            state.spread_last_logged = now


async def log_ws_health(state: AppState, configured_exchanges: list[str]) -> None:
    """Periodically log WebSocket health metrics per exchange.

    Logs every 60 seconds with:
    - Updates count per exchange
    - Unique symbols seen per exchange
    - Allocated symbols (expected from configuration)
    - Stale intervals (consecutive 60s periods with 0 updates)
    - Cross-exchange symbol coverage (symbols on 2+ exchanges)

    Args:
        state: AppState instance containing ws_metrics
        configured_exchanges: List of exchange IDs from settings (to show 0 updates)
    """
    while True:
        await asyncio.sleep(60)  # Log every 60 seconds

        async with state.metrics_lock:
            now = time.time()
            if now - state.ws_metrics.last_logged < 60:
                continue

            # Per-exchange stats (include configured exchanges even if 0 updates)
            exchange_stats = []
            for ex_id in sorted(configured_exchanges):
                updates = state.ws_metrics.update_counters.get(ex_id, 0)
                symbols = len(state.ws_metrics.symbols_seen.get(ex_id, set()))
                allocated = state.ws_metrics.allocated_symbols.get(ex_id, 0)
                _update_ws_stale_intervals(
                    state.ws_metrics.stale_intervals,
                    ex_id=ex_id,
                    allocated=allocated,
                    updates=updates,
                )
                exchange_stats.append(
                    _format_ws_exchange_stat(
                        ex_id=ex_id,
                        updates=updates,
                        unique_symbols=symbols,
                        allocated=allocated,
                        stale_intervals=state.ws_metrics.stale_intervals.get(ex_id, 0),
                    )
                )

            # Cross-exchange symbol coverage
            all_symbols_by_exchange: dict[str, set[str]] = {}
            for ex_id, symbols in state.ws_metrics.symbols_seen.items():
                all_symbols_by_exchange[ex_id] = symbols.copy()

            # Find symbols on 2+ exchanges
            symbol_exchange_count: defaultdict[str, int] = defaultdict(int)
            for ex_id, symbols in all_symbols_by_exchange.items():
                for sym in symbols:
                    symbol_exchange_count[sym] += 1

            multi_exchange_symbols = [
                sym for sym, count in symbol_exchange_count.items() if count >= 2
            ]

            logger.info(
                "WS HEALTH | %s | multi_ex_symbols=%d total_symbols=%d",
                " ".join(exchange_stats) if exchange_stats else "NO_DATA",
                len(multi_exchange_symbols),
                len(symbol_exchange_count),
            )

            # Log top 5 multi-exchange symbols for debugging
            if multi_exchange_symbols:
                top_symbols = sorted(
                    multi_exchange_symbols,
                    key=lambda s: symbol_exchange_count[s],
                    reverse=True,
                )[:5]
                exchanges_for_symbols = []
                for sym in top_symbols:
                    exs = [
                        ex
                        for ex, syms in all_symbols_by_exchange.items()
                        if sym in syms
                    ]
                    exchanges_for_symbols.append(f"{sym}({','.join(sorted(exs))})")
                logger.info("WS OVERLAP | %s", " ".join(exchanges_for_symbols))

            # Reset counters for next interval
            state.ws_metrics.update_counters.clear()
            state.ws_metrics.symbols_seen.clear()
            state.ws_metrics.last_logged = now


async def log_liquidity_summary(state: AppState) -> None:
    """Periodically log liquidity summary per exchange.

    Logs every 10 seconds with count of symbols passing liquidity filter per exchange.
    Counters are incremented in on_orderbook_update when symbol passes filter.

    Args:
        state: AppState instance containing stats
    """
    while True:
        await asyncio.sleep(10)

        async with state.metrics_lock:
            if not state.stats:
                continue

            snapshot = dict(state.stats)
            state.stats.clear()

            for ex_id, count in snapshot.items():
                logger.info("LIQ SUMMARY | %s: %d symbols passing", ex_id, count)


async def log_trader_stats(state: AppState) -> None:
    """Log trader statistics periodically.

    Logs every 30 seconds with:
    - Active cycles
    - Completed cycles
    - Failed cycles
    - Success rate

    Args:
        state: AppState instance containing trader reference
    """
    while True:
        await asyncio.sleep(30)

        if state.trader:
            stats_data = state.trader.get_stats()
            logger.info(
                "TRADER STATS | active=%d completed=%d failed=%d success_rate=%.1f%%",
                stats_data["active_cycles"],
                stats_data["total_completed"],
                stats_data["total_failed"],
                stats_data["success_rate"],
            )


async def log_memory_health(state: AppState) -> None:
    """Log memory health metrics periodically.

    Called from cleanup_stale_data task (every 300s) to track state size.
    Logs:
    - Orderbook state entries
    - Alert deduplication entries

    Args:
        state: AppState instance
    """
    async with state.orderbooks_lock:
        state_entries = len(state.orderbooks)

    async with state.alert_lock:
        alert_entries = len(state.last_alert_ts)

    logger.info(
        "MEMORY HEALTH | state_entries=%d alert_entries=%d",
        state_entries,
        alert_entries,
    )


async def cleanup_stale_data(state: AppState) -> None:
    """Remove stale entries from last_alert_ts (4h TTL) and log memory health.

    Runs every 5 minutes (reduced from 30min for memory stability).

    Args:
        state: AppState instance
    """
    while True:
        await asyncio.sleep(300)  # Every 5 min

        # Cleanup stale alerts
        removed = await state.cleanup_stale_alerts(ttl_seconds=14400)  # 4h TTL
        if removed > 0:
            logger.info("CLEANUP | Removed %d stale alert entries", removed)

        # Log memory health
        await log_memory_health(state)


def start_metrics_logger(
    state: AppState,
    configured_exchanges: list[str],
) -> list[asyncio.Task]:
    """Start all background metric logging tasks.

    Creates async tasks for:
    - Funnel metrics (60s interval)
    - Spread distribution (300s interval)
    - WebSocket health (60s interval)
    - V2 validation summary (60s interval)
    - Liquidity summary (10s interval)
    - Trader stats (30s interval)
    - Stale data cleanup (300s interval)

    Args:
        state: AppState instance to pass to metric loggers
        configured_exchanges: List of exchange IDs from settings

    Returns:
        List of created asyncio tasks (for lifecycle management)
    """
    tasks = [
        asyncio.create_task(log_validation_summary(state)),
        asyncio.create_task(log_spread_distribution(state)),
        asyncio.create_task(log_ws_health(state, configured_exchanges)),
        asyncio.create_task(log_liquidity_summary(state)),
        asyncio.create_task(log_trader_stats(state)),
        asyncio.create_task(cleanup_stale_data(state)),
        asyncio.create_task(log_truth_probe_summary(state)),
    ]

    logger.info(
        "METRICS LOGGER | Started %d background tasks: validation, spread, ws_health, liquidity, trader, cleanup, truth",
        len(tasks),
    )

    return tasks
