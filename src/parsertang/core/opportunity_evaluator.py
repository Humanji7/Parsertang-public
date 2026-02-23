"""Arbitrage opportunity evaluation and alert triggering.

Evaluates potential arbitrage opportunities by:
- Finding buy/sell exchange pairs with best prices
- Calculating fees and network selection (via fee_calculator)
- Checking profitability thresholds
- Validating slippage constraints
- Triggering alerts and trading cycles

CRITICAL ASYNC BOUNDARY FIX:
- evaluate_arbitrage_for_symbol() accepts event loop parameter to safely create tasks
- Avoids RuntimeError when called from sync context (WS callback)
- Event loop must be injected from orchestrator (main.py)

Thread Safety:
- Reads from AppState.orderbooks (thread-safe via lock)
- Updates metrics counters (lock-free, reset periodically)
- Alert deduplication via AppState.check_and_update_alert (locked)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from parsertang.alerts import format_alert, pick_alert_channel
from parsertang.truth_gate import truth_gate_status
from parsertang.arbitrage import Opportunity
from parsertang.config import settings
from parsertang.core.fee_calculator import (
    FeeCalculationResult,
    calculate_opportunity_fees_and_network,
)
from parsertang.v2.validator import ValidationResult, Validator

if TYPE_CHECKING:
    from parsertang.core.state_manager import AppState

logger = logging.getLogger(__name__)

# Deduplication constants
ARB_OK_LOG_COOLDOWN_SECONDS = 10  # Min seconds between ARB OK logs for same pair
ALERT_HARD_COOLDOWN_SECONDS = (
    30  # Absolute min seconds between alerts (spam protection)
)
ALERT_MIN_REPEAT_SECONDS = (
    300  # Manual mode: avoid repeating same pair within 5 minutes
)
ALERT_SOFT_COOLDOWN_SECONDS = 60  # Soft cooldown (can be bypassed if profit improved)

_ALERT_TRACE_SYMBOLS_RAW: str | None = None
_ALERT_TRACE_SYMBOLS_SET: set[str] = set()
_ALERT_TRACE_LAST_TS: dict[str, float] = {}
_TRUTH_PROBE_LAST_TS: dict[tuple[str, str, str], float] = {}
_TRUTH_FAIL_LAST_ALERT_TS: dict[tuple[str, str, str], float] = {}


def _get_alert_trace_symbols() -> set[str]:
    global _ALERT_TRACE_SYMBOLS_RAW, _ALERT_TRACE_SYMBOLS_SET
    raw = (settings.alert_trace_symbols or "").strip()
    if raw != (_ALERT_TRACE_SYMBOLS_RAW or ""):
        _ALERT_TRACE_SYMBOLS_RAW = raw
        _ALERT_TRACE_SYMBOLS_SET = {s.strip() for s in raw.split(",") if s.strip()}
    return _ALERT_TRACE_SYMBOLS_SET


def _should_trace_symbol(symbol: str, now: float) -> bool:
    if not settings.alert_trace_enabled:
        return False
    trace_symbols = _get_alert_trace_symbols()
    if trace_symbols and symbol not in trace_symbols:
        return False
    interval = float(max(settings.alert_trace_interval_seconds, 0))
    if interval == 0:
        return True
    last_ts = _ALERT_TRACE_LAST_TS.get(symbol, 0.0)
    if now - last_ts < interval:
        return False
    _ALERT_TRACE_LAST_TS[symbol] = now
    return True


def _should_truth_probe(key: tuple[str, str, str], now: float) -> bool:
    if not settings.v2_truth_probe_enabled:
        return False
    interval = float(max(settings.v2_truth_probe_interval_seconds, 0))
    if interval == 0:
        return True
    last_ts = _TRUTH_PROBE_LAST_TS.get(key, 0.0)
    if now - last_ts < interval:
        return False
    _TRUTH_PROBE_LAST_TS[key] = now
    return True


def _should_truth_fail_alert(key: tuple[str, str, str], now: float) -> bool:
    if not settings.v2_truth_fail_tech_alert_enabled:
        return False
    interval = float(max(settings.v2_truth_fail_tech_alert_interval_seconds, 0.0))
    if interval == 0:
        return True
    last_ts = _TRUTH_FAIL_LAST_ALERT_TS.get(key, 0.0)
    if now - last_ts < interval:
        return False
    _TRUTH_FAIL_LAST_ALERT_TS[key] = now
    return True


def should_send_alert(
    *,
    now: float,
    last_alert: tuple[float, float] | None,
    net_profit_pct: float,
    dedup_threshold_pct: float,
    min_repeat_seconds: float,
    hard_cooldown_seconds: float,
    soft_cooldown_seconds: float,
) -> bool:
    """Return True if alert should be sent based on cooldown and profit change."""
    if last_alert is None:
        return True

    last_ts, last_net_profit = last_alert
    elapsed = now - last_ts
    if elapsed < min_repeat_seconds:
        return False

    if elapsed < hard_cooldown_seconds:
        return False

    if elapsed < soft_cooldown_seconds:
        profit_improved = (net_profit_pct - last_net_profit) > dedup_threshold_pct
        return profit_improved

    return True


def _effective_net_profit_pct(fee: FeeCalculationResult) -> float:
    if settings.profit_mode == "funded":
        return float(getattr(fee, "net_profit_funded_pct", 0.0))
    return float(fee.net_profit_pct)


def evaluate_arbitrage_for_symbol(
    symbol: str,
    state: AppState,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Evaluate arbitrage opportunity for symbol across all exchanges.

    Finds best buy/sell pair, calculates fees, validates profitability,
    and triggers alerts/trading if thresholds are met.

    ASYNC BOUNDARY FIX: Accepts event loop parameter to safely create tasks
    from sync context (WS callback). Loop must be injected by orchestrator.

    Args:
        symbol: Trading pair to evaluate (e.g., "BTC/USDT")
        state: Application state containing orderbooks, alerts, trader
        loop: Event loop for creating async tasks (trader.start_cycle)

    Side Effects:
        - Increments funnel_counters (arb_ok, arb_skip, arb_reject_*)
        - Updates spread_buckets for distribution analysis
        - Sends alerts via state.alert_service (if enabled)
        - Creates trader tasks via loop.create_task (if enabled)
        - Updates last_alert_ts for deduplication

    Thread Safety:
        - Direct read from state.orderbooks (no lock — fast path)
        - Metrics counters are lock-free (reset periodically)
        - Alert dedup uses state.check_and_update_alert (locked)
    """
    now_wall = time.time()
    blacklist_until = state.validation_symbol_blacklist.get(symbol)
    if blacklist_until:
        if now_wall < blacklist_until:
            state.funnel_counters["arb_skip_blacklist"] += 1
            return
        state.validation_symbol_blacklist.pop(symbol, None)

    # Collect fresh snapshots for this symbol
    # Direct access to state.orderbooks (no lock — accepting race condition for performance)
    snapshots: list[tuple[str, dict[str, float]]] = []
    for (ex_id, sym), snapshot in state.orderbooks.items():
        if sym != symbol:
            continue
        if now_wall - snapshot.ts > settings.orderbook_stale_seconds:
            state.funnel_counters["arb_skip_stale"] += 1
            continue

        # Convert snapshot to dict for compatibility with existing code
        snapshot_dict = {
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "bid_liq_usd": snapshot.bid_liq_usd,
            "ask_liq_usd": snapshot.ask_liq_usd,
            "bid_slip_pct": snapshot.bid_slip_pct,
            "ask_slip_pct": snapshot.ask_slip_pct,
            "ts": snapshot.ts,
        }
        snapshots.append((ex_id, snapshot_dict))

    # Side-specific liquidity:
    # - buy exchange needs ASK liquidity
    # - sell exchange needs BID liquidity
    #
    # Requiring both sides on each exchange is overly strict and drops viable
    # opportunities (especially when one exchange has thin bids but deep asks,
    # or vice versa).
    threshold = float(settings.liquidity_usd_threshold)
    buy_candidates = [
        (ex_id, snap)
        for ex_id, snap in snapshots
        if float(snap.get("ask_liq_usd", 0.0) or 0.0) >= threshold
    ]
    sell_candidates = [
        (ex_id, snap)
        for ex_id, snap in snapshots
        if float(snap.get("bid_liq_usd", 0.0) or 0.0) >= threshold
    ]

    # Need at least one viable buy and one viable sell on distinct exchanges
    best_pair: tuple[str, dict[str, float], str, dict[str, float]] | None = None
    best_gross_spread_pct: float | None = None
    for buy_ex, buy_snap in buy_candidates:
        best_ask = float(buy_snap.get("best_ask", 0.0) or 0.0)
        if best_ask <= 0:
            continue
        for sell_ex, sell_snap in sell_candidates:
            if sell_ex == buy_ex:
                continue
            best_bid = float(sell_snap.get("best_bid", 0.0) or 0.0)
            gross_spread_pct = ((best_bid - best_ask) / best_ask) * 100.0
            if best_gross_spread_pct is None or gross_spread_pct > best_gross_spread_pct:
                best_gross_spread_pct = gross_spread_pct
                best_pair = (buy_ex, buy_snap, sell_ex, sell_snap)

    if best_pair is None:
        now = time.monotonic()
        if _should_trace_symbol(symbol, now):
            buy_ex = ",".join(ex_id for ex_id, _ in buy_candidates) or "-"
            sell_ex = ",".join(ex_id for ex_id, _ in sell_candidates) or "-"
            # IMPORTANT: log the exact snapshots used for candidate filtering.
            # Re-reading `state.orderbooks` here can be misleading under concurrent
            # updates (and includes stale snapshots we already filtered out).
            snapshot_details: list[str] = []
            for ex_id, snap in sorted(snapshots, key=lambda x: x[0]):
                ts = float(snap.get("ts", 0.0) or 0.0)
                age_s = (now_wall - ts) if ts > 0 else -1.0
                bid = float(snap.get("best_bid", 0.0) or 0.0)
                ask = float(snap.get("best_ask", 0.0) or 0.0)
                bid_liq = float(snap.get("bid_liq_usd", 0.0) or 0.0)
                ask_liq = float(snap.get("ask_liq_usd", 0.0) or 0.0)
                buy_ok = "Y" if ask_liq >= threshold else "n"
                sell_ok = "Y" if bid_liq >= threshold else "n"
                snapshot_details.append(
                    f"{ex_id} bid={bid:.5f} ask={ask:.5f} liq={bid_liq:.0f}/{ask_liq:.0f} "
                    f"age={age_s:.2f}s ok={buy_ok}/{sell_ok}"
                )
            details = "; ".join(snapshot_details) or "-"
            logger.info(
                "ARB TRACE | %s reason=insufficient_exchanges eligible=%d stale<=%.1fs liq>=%.0f buy_ex=%s sell_ex=%s snapshots=%s",
                symbol,
                len(snapshots),
                float(settings.orderbook_stale_seconds),
                float(threshold),
                buy_ex,
                sell_ex,
                details,
            )
        return

    buy_exchange, buy_snapshot, sell_exchange, sell_snapshot = best_pair

    # Can't arbitrage on same exchange
    if buy_exchange == sell_exchange:
        return

    best_ask = buy_snapshot["best_ask"]
    best_bid = sell_snapshot["best_bid"]

    # Sanity check: bid must be higher than ask for profit
    if best_bid <= best_ask or best_ask <= 0:
        now = time.monotonic()
        if _should_trace_symbol(symbol, now):
            now_wall = time.time()
            buy_ts = buy_snapshot.get("ts", 0.0) or 0.0
            sell_ts = sell_snapshot.get("ts", 0.0) or 0.0
            buy_age_s = now_wall - buy_ts if buy_ts > 0 else -1.0
            sell_age_s = now_wall - sell_ts if sell_ts > 0 else -1.0
            max_age_s = max(buy_age_s, sell_age_s)
            age_skew_s = abs(buy_ts - sell_ts) if buy_ts and sell_ts else -1.0
            stale = (
                max_age_s > settings.alert_trace_stale_seconds
                if max_age_s >= 0
                else False
            )
            gross_spread_pct = ((best_bid - best_ask) / best_ask) * 100.0
            logger.info(
                "ARB TRACE | %s buy=%s ask=%.5f sell=%s bid=%.5f gross=%.3f%% reason=no_arb "
                "liq_bid=%.0f liq_ask=%.0f slip_buy=%.3f%% slip_sell=%.3f%% "
                "ts_buy=%.3f ts_sell=%.3f age_buy=%.3fs age_sell=%.3fs skew=%.3fs stale=%s",
                symbol,
                buy_exchange,
                best_ask,
                sell_exchange,
                best_bid,
                gross_spread_pct,
                sell_snapshot.get("bid_liq_usd", 0.0),
                buy_snapshot.get("ask_liq_usd", 0.0),
                buy_snapshot.get("ask_slip_pct", 0.0),
                sell_snapshot.get("bid_slip_pct", 0.0),
                buy_ts,
                sell_ts,
                buy_age_s,
                sell_age_s,
                age_skew_s,
                "yes" if stale else "no",
            )
        return

    # Calculate fees and network selection (extracted to fee_calculator module)
    result = calculate_opportunity_fees_and_network(
        symbol=symbol,
        buy_exchange=buy_exchange,
        sell_exchange=sell_exchange,
        best_ask=best_ask,
        best_bid=best_bid,
        state=state,
    )

    # Handle validation errors — skip with aggregated logging
    if not result.is_valid:
        state.funnel_counters["arb_skip"] += 1
        reason = result.error_reason or "unknown"
        state.arb_skip_reasons[reason] += 1
        state.arb_skip_samples.setdefault(reason, (symbol, buy_exchange, sell_exchange))

        # Track currencies needing fallback data (for daily report)
        if state.fee_stats and reason in (
            "currency_not_available_buy",
            "currency_not_available_sell",
            "no_withdrawal_fee",
        ):
            base_currency = symbol.split("/")[0]
            exchange = buy_exchange if "buy" in reason else sell_exchange
            loop.create_task(
                state.fee_stats.record_missing_fallback(exchange, base_currency)
            )

        # TRUTH ratio must reflect correctness of alertable signals.
        # Fee-calculation misses are valuable diagnostics, but they should not
        # participate in the TRUTH OK/FAIL ratio gate.
        if settings.v2_truth_probe_enabled and settings.v2_validation_enabled:
            alert_key = (symbol, buy_exchange, sell_exchange)
            now = time.monotonic()
            truth_probe = _should_truth_probe(alert_key, now)
            if truth_probe:
                validator = Validator(state.gateway)
                validation = validator.validate(
                    symbol=symbol,
                    buy_ex=buy_exchange,
                    sell_ex=sell_exchange,
                    buy_price=best_ask,
                    sell_price=best_bid,
                )
                if not validation.ok:
                    logger.info(
                        "TRUTH PROBE | %s buy=%s sell=%s ok=False reason=fee_calc_%s ws_buy=%.6f ws_sell=%.6f rest_buy=%s rest_sell=%s",
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        validation.reason,
                        best_ask,
                        best_bid,
                        f"{validation.rest_buy:.6f}" if validation.rest_buy else "-",
                        f"{validation.rest_sell:.6f}" if validation.rest_sell else "-",
                    )
                    if state.alert_service:
                        now = time.monotonic()
                        if _should_truth_fail_alert(alert_key, now):
                            loop.create_task(
                                state.alert_service.send_tech(
                                    f"TRUTH PROBE | {symbol} {buy_exchange}→{sell_exchange} reason=fee_calc_{validation.reason} "
                                    f"ws_buy={best_ask:.6f} ws_sell={best_bid:.6f} "
                                    f"rest_buy={validation.rest_buy or 0.0:.6f} rest_sell={validation.rest_sell or 0.0:.6f}"
                                )
                            )
                else:
                    logger.info(
                        "TRUTH PROBE | %s buy=%s sell=%s ok=True reason=fee_calc_ok rest_buy=%.6f rest_sell=%.6f",
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        validation.rest_buy or 0.0,
                        validation.rest_sell or 0.0,
                    )
        return

    # Extract fee calculation results
    network = result.network
    withdraw_fee_base = result.withdraw_fee_base  # Fee in BASE currency
    buy_fee_pct = result.buy_fee_pct
    sell_fee_pct = result.sell_fee_pct
    withdraw_fee_pct = result.withdraw_fee_pct
    net_profit_transfer_pct = float(result.net_profit_pct)
    net_profit_funded_pct = float(getattr(result, "net_profit_funded_pct", 0.0))
    net_profit_pct = (
        net_profit_funded_pct
        if settings.profit_mode == "funded"
        else net_profit_transfer_pct
    )
    total_fees_pct = result.total_fees_pct
    fee_confidence = result.fee_confidence

    gross_spread_pct = ((best_bid - best_ask) / best_ask) * 100.0
    trade_fees_pct = buy_fee_pct + sell_fee_pct

    def _run_v2_validation(
        should_alert: bool, truth_probe: bool, *, ws_now: float
    ) -> tuple[bool, ValidationResult | None, FeeCalculationResult | None]:
        if not (should_alert or truth_probe):
            return True, None, None
        if not settings.v2_validation_enabled:
            return True, None, None

        wall_now = time.time()
        buy_ts = buy_snapshot.get("ts", 0.0) or 0.0
        sell_ts = sell_snapshot.get("ts", 0.0) or 0.0
        age_buy_ms = int((ws_now - buy_ts) * 1000) if buy_ts > 0 else -1
        age_sell_ms = int((ws_now - sell_ts) * 1000) if sell_ts > 0 else -1
        skew_ms = int(abs(buy_ts - sell_ts) * 1000) if buy_ts and sell_ts else -1

        validation: ValidationResult | None = None
        if buy_ts <= 0 or sell_ts <= 0:
            validation = ValidationResult(ok=False, reason="ws_missing_ts")
        elif (
            age_buy_ms > settings.v2_validation_ws_max_age_ms
            or age_sell_ms > settings.v2_validation_ws_max_age_ms
        ):
            validation = ValidationResult(ok=False, reason="ws_stale")
        elif skew_ms > settings.v2_validation_ws_max_skew_ms:
            validation = ValidationResult(ok=False, reason="ws_skew")
        elif settings.use_dynamic_withdrawal_fees:
            if not state.fee_manager or not state.fee_manager.cache:
                validation = ValidationResult(ok=False, reason="fee_cache_missing")
            else:
                cache_age = wall_now - state.fee_manager.cache.last_updated
                if cache_age > settings.v2_validation_fee_max_age_seconds:
                    validation = ValidationResult(ok=False, reason="fee_cache_stale")

        if validation is None:
            validator = Validator(state.gateway)
            validation = validator.validate(
                symbol=symbol,
                buy_ex=buy_exchange,
                sell_ex=sell_exchange,
                buy_price=best_ask,
                sell_price=best_bid,
            )
        if not validation.ok:
            if validation.reason in {"ws_stale", "ws_skew", "ws_missing_ts"}:
                count = state.validation_stale_counts[symbol] + 1
                state.validation_stale_counts[symbol] = count
                if (
                    settings.v2_validation_stale_symbol_threshold > 0
                    and count
                    >= settings.v2_validation_stale_symbol_threshold
                ):
                    cooldown = settings.v2_validation_stale_symbol_cooldown_seconds
                    if cooldown > 0:
                        state.validation_symbol_blacklist[symbol] = wall_now + cooldown
                        logger.info(
                            "SYMBOL BLACKLIST | %s reason=%s cooldown=%ds",
                            symbol,
                            validation.reason,
                            cooldown,
                        )
                    state.validation_stale_counts[symbol] = 0
            elif validation.reason.startswith("rest_") or validation.reason in {
                "rest_error",
                "rest_empty",
            }:
                count = state.validation_rest_fail_counts[symbol] + 1
                state.validation_rest_fail_counts[symbol] = count
                if (
                    settings.v2_validation_rest_symbol_threshold > 0
                    and count >= settings.v2_validation_rest_symbol_threshold
                ):
                    cooldown = settings.v2_validation_rest_symbol_cooldown_seconds
                    if cooldown > 0:
                        state.validation_symbol_blacklist[symbol] = wall_now + cooldown
                        logger.info(
                            "SYMBOL BLACKLIST | %s reason=%s cooldown=%ds",
                            symbol,
                            validation.reason,
                            cooldown,
                        )
                    state.validation_rest_fail_counts[symbol] = 0
            buy_diff_pct = None
            sell_diff_pct = None
            if validation.rest_buy and best_ask:
                buy_diff_pct = (validation.rest_buy - best_ask) / best_ask * 100
            if validation.rest_sell and best_bid:
                sell_diff_pct = (best_bid - validation.rest_sell) / best_bid * 100
            state.funnel_counters["alerts_invalid"] += 1
            logger.info(
                "V2 VALIDATION | %s buy=%s sell=%s ok=%s reason=%s ws_buy=%.6f ws_sell=%.6f "
                "rest_buy=%s rest_sell=%s buy_diff_pct=%s sell_diff_pct=%s ws_age_ms=%s/%s tol_pct=%.3f",
                symbol,
                buy_exchange,
                sell_exchange,
                validation.ok,
                validation.reason,
                best_ask,
                best_bid,
                f"{validation.rest_buy:.6f}" if validation.rest_buy else "-",
                f"{validation.rest_sell:.6f}" if validation.rest_sell else "-",
                f"{buy_diff_pct:.3f}" if buy_diff_pct is not None else "-",
                f"{sell_diff_pct:.3f}" if sell_diff_pct is not None else "-",
                age_buy_ms,
                age_sell_ms,
                settings.v2_validation_price_tolerance_pct,
            )

            # TRUTH ratio should measure "do we lie in alerts" (WS↔REST correctness),
            # not "is this opportunity executable right now".
            #
            # Liquidity/depth rejects are valid blockers, but they don't indicate a
            # WS/REST inconsistency. Count them as TRUTH OK (blocked), similar to
            # rest_net_below_threshold_blocked.
            if validation.reason in {
                "rest_ask_liq",
                "rest_bid_liq",
                "rest_ask_depth",
                "rest_bid_depth",
            }:
                state.funnel_counters["truth_ok"] += 1
                logger.info(
                    "TRUTH OK | %s buy=%s sell=%s reason=%s_blocked ws_buy=%.6f ws_sell=%.6f rest_buy=%s rest_sell=%s",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    validation.reason,
                    best_ask,
                    best_bid,
                    f"{validation.rest_buy:.6f}" if validation.rest_buy else "-",
                    f"{validation.rest_sell:.6f}" if validation.rest_sell else "-",
                )
                return False, validation, None

            state.funnel_counters["truth_fail"] += 1
            logger.info(
                "TRUTH FAIL | %s buy=%s sell=%s reason=%s ws_buy=%.6f ws_sell=%.6f rest_buy=%s rest_sell=%s",
                symbol,
                buy_exchange,
                sell_exchange,
                validation.reason,
                best_ask,
                best_bid,
                f"{validation.rest_buy:.6f}" if validation.rest_buy else "-",
                f"{validation.rest_sell:.6f}" if validation.rest_sell else "-",
            )
            if state.alert_service:
                alert_key = (symbol, buy_exchange, sell_exchange)
                now = time.monotonic()
                if _should_truth_fail_alert(alert_key, now):
                    loop.create_task(
                        state.alert_service.send_tech(
                            f"TRUTH FAIL | {symbol} {buy_exchange}→{sell_exchange} reason={validation.reason} "
                            f"ws_buy={best_ask:.6f} ws_sell={best_bid:.6f} "
                            f"rest_buy={validation.rest_buy or 0.0:.6f} rest_sell={validation.rest_sell or 0.0:.6f}"
                        )
                    )
                return False, validation, None
            return False, validation, None
        state.funnel_counters["alerts_valid"] += 1
        state.validation_stale_counts.pop(symbol, None)
        state.validation_rest_fail_counts.pop(symbol, None)

        # Fee validation on REST snapshot prices (trade + withdrawal)
        fee_rest: FeeCalculationResult | None = None
        if validation.rest_buy and validation.rest_sell:
            fee_rest = calculate_opportunity_fees_and_network(
                symbol=symbol,
                buy_exchange=buy_exchange,
                sell_exchange=sell_exchange,
                best_ask=validation.rest_buy,
                best_bid=validation.rest_sell,
                state=state,
            )
            if not fee_rest.is_valid:
                state.funnel_counters["fee_validation_fail"] += 1
                reason = fee_rest.error_reason or "unknown"
                state.fee_validation_reasons[reason] += 1
                state.fee_validation_samples.setdefault(
                    reason, (symbol, buy_exchange, sell_exchange)
                )
                logger.info(
                    "FEE VALIDATION | %s buy=%s sell=%s ok=False reason=%s",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    fee_rest.error_reason or "unknown",
                )
                state.funnel_counters["truth_fail"] += 1
                logger.info(
                    "TRUTH FAIL | %s buy=%s sell=%s reason=%s rest_buy=%.6f rest_sell=%.6f",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    fee_rest.error_reason or "unknown",
                    validation.rest_buy,
                    validation.rest_sell,
                )
                if state.alert_service:
                    alert_key = (symbol, buy_exchange, sell_exchange)
                    now = time.monotonic()
                    if _should_truth_fail_alert(alert_key, now):
                        loop.create_task(
                            state.alert_service.send_tech(
                                f"TRUTH FAIL | {symbol} {buy_exchange}→{sell_exchange} reason={fee_rest.error_reason or 'unknown'} "
                                f"rest_buy={validation.rest_buy:.6f} rest_sell={validation.rest_sell:.6f}"
                            )
                        )
                return False, validation, fee_rest
            rest_net_effective = _effective_net_profit_pct(fee_rest)
            if rest_net_effective < settings.min_net_profit:
                ws_net_profit_pct = float(net_profit_pct)
                rest_net_profit_pct = float(rest_net_effective)
                rest_buy = float(validation.rest_buy)
                rest_sell = float(validation.rest_sell)
                rest_gross_spread_pct = (
                    ((rest_sell - rest_buy) / rest_buy) * 100.0 if rest_buy > 0 else 0.0
                )
                rest_trade_fee_pct = float(fee_rest.buy_fee_pct + fee_rest.sell_fee_pct)
                rest_withdraw_fee_pct = float(fee_rest.withdraw_fee_pct)
                rest_fees_pct = rest_trade_fee_pct + rest_withdraw_fee_pct

                # Profitability threshold mismatch should NOT tank global TRUTH ratio.
                # TRUTH is about "do we send false alerts", and V2 validation blocks this case.
                state.funnel_counters["fee_validation_fail"] += 1
                state.fee_validation_reasons["rest_net_below_threshold"] += 1
                state.fee_validation_samples.setdefault(
                    "rest_net_below_threshold",
                    (symbol, buy_exchange, sell_exchange),
                )
                logger.info(
                    "FEE VALIDATION | %s buy=%s sell=%s ok=False reason=rest_net_below_threshold "
                    "net=%.3f%% < %.2f%% gross=%.3f%% fees=%.3f%% (trade=%.3f%% wd=%.3f%%) netw=%s mode=%s",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    rest_net_profit_pct,
                    settings.min_net_profit,
                    rest_gross_spread_pct,
                    rest_fees_pct,
                    rest_trade_fee_pct,
                    rest_withdraw_fee_pct,
                    fee_rest.network or "N/A",
                    settings.profit_mode,
                )
                state.funnel_counters["truth_ok"] += 1
                logger.info(
                    "TRUTH OK | %s buy=%s sell=%s reason=rest_net_below_threshold_blocked "
                    "ws_net=%.3f%% rest_net=%.3f%% rest_buy=%.6f rest_sell=%.6f",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    ws_net_profit_pct,
                    rest_net_profit_pct,
                    validation.rest_buy,
                    validation.rest_sell,
                )
                return False, validation, fee_rest
            state.funnel_counters["fee_validation_ok"] += 1
            logger.info(
                "FEE SNAPSHOT | %s buy=%s sell=%s trade=%.3f%% wd=%.3f%% net=%.3f%% netw=%s",
                symbol,
                buy_exchange,
                sell_exchange,
                fee_rest.buy_fee_pct + fee_rest.sell_fee_pct,
                fee_rest.withdraw_fee_pct,
                rest_net_effective,
                fee_rest.network or "N/A",
            )
            state.funnel_counters["truth_ok"] += 1
            logger.info(
                "TRUTH OK | %s buy=%s sell=%s net=%.3f%% rest_buy=%.6f rest_sell=%.6f netw=%s",
                symbol,
                buy_exchange,
                sell_exchange,
                rest_net_effective,
                validation.rest_buy,
                validation.rest_sell,
                fee_rest.network or "N/A",
            )
            logger.info(
                "SIGNAL SNAPSHOT | %s ws_buy=%.6f ws_sell=%.6f rest_buy=%.6f rest_sell=%.6f trade=%.3f%% wd=%.3f%% net=%.3f%% netw=%s",
                symbol,
                best_ask,
                best_bid,
                validation.rest_buy,
                validation.rest_sell,
                fee_rest.buy_fee_pct + fee_rest.sell_fee_pct,
                fee_rest.withdraw_fee_pct,
                rest_net_effective,
                fee_rest.network or "N/A",
            )
        return True, validation, fee_rest

    # Track spread distribution for market analysis
    if gross_spread_pct < 0:
        state.spread_buckets["negative"] += 1
    elif gross_spread_pct < 0.1:
        state.spread_buckets["0-0.1"] += 1
    elif gross_spread_pct < 0.3:
        state.spread_buckets["0.1-0.3"] += 1
    elif gross_spread_pct < 0.5:
        state.spread_buckets["0.3-0.5"] += 1
    else:
        state.spread_buckets[">0.5"] += 1

    v2_prefetch: tuple[bool, ValidationResult | None, FeeCalculationResult | None] | None = None

    # Threshold check: reject if below minimum profit
    if net_profit_pct <= settings.min_net_profit:
        state.funnel_counters["arb_reject_threshold"] += 1

        # Diagnostic logging (sampled to reduce spam)
        base_currency = symbol.split("/")[0]
        withdraw_fee_usd = withdraw_fee_base * best_ask

        reject_count = state.funnel_counters["arb_reject_threshold"]
        log_level = logger.info if reject_count % 100 == 0 else logger.debug
        log_level(
            "ARB REJECT | %s %s→%s gross=%.3f%% fees=%.3f%% (trade:%.2f%% wd:%.3f%% [%.4f %s = $%.4f]) "
            "net=%.3f%% < %.2f%% (threshold) netw=%s confidence=%s",
            symbol,
            buy_exchange,
            sell_exchange,
            gross_spread_pct,
            total_fees_pct,
            trade_fees_pct,
            withdraw_fee_pct,
            withdraw_fee_base,
            base_currency,
            withdraw_fee_usd,
            net_profit_pct,
            settings.min_net_profit,
            network or "N/A",
            fee_confidence,
        )
        now = time.monotonic()
        alert_key = (symbol, buy_exchange, sell_exchange)
        truth_probe = _should_truth_probe(alert_key, now)
        if truth_probe:
            v2_prefetch = _run_v2_validation(False, truth_probe, ws_now=now_wall)
            v2_ok, v2_validation, v2_fee_rest = v2_prefetch
            if not v2_ok:
                return

            # PROMOTION: If REST validation finds a truly profitable opportunity, do not
            # discard it just because WS-based fee math was below threshold.
            if (
                v2_fee_rest
                and v2_fee_rest.is_valid
                and _effective_net_profit_pct(v2_fee_rest) >= settings.min_net_profit
                and v2_validation
                and v2_validation.rest_buy
                and v2_validation.rest_sell
            ):
                logger.info(
                    "TRUTH PROBE PROMOTE | %s buy=%s sell=%s ws_net=%.3f%% rest_net=%.3f%%",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    net_profit_pct,
                    float(_effective_net_profit_pct(v2_fee_rest)),
                )
                best_ask = float(v2_validation.rest_buy)
                best_bid = float(v2_validation.rest_sell)
                gross_spread_pct = ((best_bid - best_ask) / best_ask) * 100.0 if best_ask > 0 else 0.0
                trade_fees_pct = float(v2_fee_rest.buy_fee_pct + v2_fee_rest.sell_fee_pct)
                withdraw_fee_pct = float(v2_fee_rest.withdraw_fee_pct)
                total_fees_pct = trade_fees_pct + withdraw_fee_pct
                net_profit_pct = float(_effective_net_profit_pct(v2_fee_rest))
                network = v2_fee_rest.network
                withdraw_fee_base = float(v2_fee_rest.withdraw_fee_base)
                fee_confidence = v2_fee_rest.fee_confidence
            else:
                return
        else:
            return

    # Slippage check: reject if expected CEX slippage exceeds budget
    buy_slip_pct = buy_snapshot.get("ask_slip_pct")
    sell_slip_pct = sell_snapshot.get("bid_slip_pct")
    if isinstance(buy_slip_pct, (int, float)) and isinstance(
        sell_slip_pct, (int, float)
    ):
        slippage_total_pct = float(buy_slip_pct) + float(sell_slip_pct)
        slippage_budget_pct = (
            max(0.0, net_profit_pct) * settings.slippage_budget_fraction
        )
        if slippage_total_pct > slippage_budget_pct:
            state.funnel_counters["arb_reject_slippage"] += 1
            logger.debug(
                "ARB REJECT SLIP | %s %s→%s net=%.3f%% slip=%.3f%% budget=%.3f%% (fraction=%.2f)",
                symbol,
                buy_exchange,
                sell_exchange,
                net_profit_pct,
                slippage_total_pct,
                slippage_budget_pct,
                settings.slippage_budget_fraction,
            )
            return
        state.funnel_counters["slip_ok"] += 1

    # Opportunity is profitable — log with deduplication (prevents 90 logs/sec spam)
    state.funnel_counters["arb_ok"] += 1
    now = time.monotonic()
    log_key = (symbol, buy_exchange, sell_exchange)
    last_log_time = state.last_arb_ok_log.get(log_key)

    # Periodic cleanup of old entries (memory leak protection)
    if len(state.last_arb_ok_log) > 500:
        cutoff = now - 300  # Remove entries older than 5 minutes
        state.last_arb_ok_log = {
            k: v for k, v in state.last_arb_ok_log.items() if v > cutoff
        }

    should_log = (
        last_log_time is None or (now - last_log_time) >= ARB_OK_LOG_COOLDOWN_SECONDS
    )

    if should_log:
        logger.info(
            "ARB OK | %s buy=%s@%.2f sell=%s@%.2f gross=%.3f%% fees=%.3f%% net=%.3f%% netw=%s confidence=%s",
            symbol,
            buy_exchange,
            best_ask,
            sell_exchange,
            best_bid,
            gross_spread_pct,
            total_fees_pct,
            net_profit_pct,
            network or "N/A",
            fee_confidence,
        )
        state.last_arb_ok_log[log_key] = now

    # Alert deduplication with min-repeat + hard/soft cooldown
    # - 0-300s: never alert (manual mode: avoid repeat pairs)
    # - 0-30s: never alert (hard cooldown, spam protection)
    # - 30-60s: alert only if profit IMPROVED by >threshold
    # - 60s+: always alert (status update)
    alert_key = (symbol, buy_exchange, sell_exchange)
    last_alert = state.last_alert_ts.get(alert_key)

    should_alert = should_send_alert(
        now=now,
        last_alert=last_alert,
        net_profit_pct=net_profit_pct,
        dedup_threshold_pct=settings.alert_dedup_threshold_pct,
        min_repeat_seconds=ALERT_MIN_REPEAT_SECONDS,
        hard_cooldown_seconds=ALERT_HARD_COOLDOWN_SECONDS,
        soft_cooldown_seconds=ALERT_SOFT_COOLDOWN_SECONDS,
    )
    if not should_alert and last_alert is not None:
        last_ts, last_net_profit = last_alert
        elapsed = now - last_ts
        profit_improved = (
            net_profit_pct - last_net_profit
        ) > settings.alert_dedup_threshold_pct

        if elapsed < ALERT_MIN_REPEAT_SECONDS:
            state.funnel_counters["alerts_min_repeat"] += 1
        elif elapsed < ALERT_HARD_COOLDOWN_SECONDS:
            state.funnel_counters["alerts_hard_cooldown"] += 1
        elif elapsed < ALERT_SOFT_COOLDOWN_SECONDS and not profit_improved:
            state.funnel_counters["alerts_soft_cooldown"] += 1

    truth_probe = (not should_alert) and _should_truth_probe(alert_key, now)

    # Validate regardless of alert service presence when dedup passes,
    # or when truth-probe sampling is enabled.
    if v2_prefetch is None:
        v2_ok, v2_validation, v2_fee_rest = _run_v2_validation(
            should_alert, truth_probe, ws_now=now_wall
        )
    else:
        v2_ok, v2_validation, v2_fee_rest = v2_prefetch
    if (should_alert or truth_probe) and not v2_ok:
        return

    # Promotion: if WS math was below threshold but REST validation confirms a
    # profitable opportunity, allow sending an alert using REST-anchored numbers.
    #
    # This increases true-alert throughput without weakening TRUTH, and avoids
    # "silent but healthy" periods caused by WS micro-skew/mismatch.
    alert_best_ask = best_ask
    alert_best_bid = best_bid
    alert_network = network
    alert_withdraw_fee_base = withdraw_fee_base
    alert_trade_fees_pct = trade_fees_pct
    alert_withdraw_fee_pct = withdraw_fee_pct
    alert_net_profit_transfer_pct = net_profit_transfer_pct
    alert_net_profit_funded_pct = net_profit_funded_pct
    alert_net_profit_pct = net_profit_pct
    alert_fee_confidence = fee_confidence

    if (
        not should_alert
        and truth_probe
        and state.alert_service
        and v2_validation
        and v2_validation.rest_buy
        and v2_validation.rest_sell
        and v2_fee_rest
        and v2_fee_rest.is_valid
    ):
        rest_net_effective = float(_effective_net_profit_pct(v2_fee_rest))
        if rest_net_effective >= float(settings.min_net_profit):
            should_alert = should_send_alert(
                now=now,
                last_alert=state.last_alert_ts.get(alert_key),
                net_profit_pct=rest_net_effective,
                dedup_threshold_pct=settings.alert_dedup_threshold_pct,
                min_repeat_seconds=ALERT_MIN_REPEAT_SECONDS,
                hard_cooldown_seconds=ALERT_HARD_COOLDOWN_SECONDS,
                soft_cooldown_seconds=ALERT_SOFT_COOLDOWN_SECONDS,
            )
            if should_alert:
                alert_best_ask = float(v2_validation.rest_buy)
                alert_best_bid = float(v2_validation.rest_sell)
                alert_network = v2_fee_rest.network
                alert_withdraw_fee_base = float(v2_fee_rest.withdraw_fee_base)
                alert_trade_fees_pct = float(v2_fee_rest.buy_fee_pct + v2_fee_rest.sell_fee_pct)
                alert_withdraw_fee_pct = float(v2_fee_rest.withdraw_fee_pct)
                alert_net_profit_transfer_pct = float(v2_fee_rest.net_profit_pct)
                alert_net_profit_funded_pct = float(getattr(v2_fee_rest, "net_profit_funded_pct", 0.0))
                alert_net_profit_pct = rest_net_effective
                alert_fee_confidence = v2_fee_rest.fee_confidence
                logger.info(
                    "ALERT PROMOTED | %s %s→%s ws_net=%.3f%% rest_net=%.3f%%",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    net_profit_pct,
                    rest_net_effective,
                )

    # Send alert if passed deduplication
    if should_alert and state.alert_service:
        state.funnel_counters["alerts_candidate"] += 1
        if settings.truth_allowlist_path:
            from parsertang.allowlist import truth_allowlist_cache

            allowed = truth_allowlist_cache.get(
                settings.truth_allowlist_path,
                refresh_seconds=settings.truth_allowlist_refresh_seconds,
            )
            if not allowed or symbol.upper() not in allowed:
                state.funnel_counters["alerts_truth_allowlist_blocked"] += 1
                logger.info(
                    "ALERT SUPPRESSED | truth_allowlist=on %s buy=%s sell=%s net=%.3f%%",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    alert_net_profit_pct,
                )
                if settings.telegram_send_suppressed_alerts_to_tech and hasattr(
                    state.alert_service, "send_tech"
                ):
                    try:
                        loop.create_task(
                            state.alert_service.send_tech(
                                f"ALERT SUPPRESSED | {symbol} {buy_exchange}→{sell_exchange} "
                                f"reason=truth_allowlist net={alert_net_profit_pct:.3f}%"
                            )
                        )
                    except Exception:
                        pass
                state.last_alert_ts[alert_key] = (now, alert_net_profit_pct)
                return

        if settings.truth_gate_enabled:
            allows, ratio, reason = truth_gate_status(
                settings.truth_gate_summary_path,
                ratio_min=settings.truth_gate_ratio_min,
                max_age_seconds=settings.truth_gate_max_age_seconds,
                min_total=settings.truth_gate_min_total,
                refresh_seconds=settings.truth_gate_refresh_seconds,
            )
            if not allows:
                state.funnel_counters["alerts_truth_gate_blocked"] += 1
                logger.info(
                    "ALERT SUPPRESSED | truth_gate=%s ratio=%.2f%% reason=%s %s buy=%s sell=%s net=%.3f%%",
                    "off",
                    ratio,
                    reason,
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    alert_net_profit_pct,
                )
                if settings.telegram_send_suppressed_alerts_to_tech and hasattr(
                    state.alert_service, "send_tech"
                ):
                    try:
                        loop.create_task(
                            state.alert_service.send_tech(
                                f"ALERT SUPPRESSED | {symbol} {buy_exchange}→{sell_exchange} "
                                f"reason=truth_gate_{reason} ratio={ratio:.2f}% net={alert_net_profit_pct:.3f}%"
                            )
                        )
                    except Exception:
                        pass
                state.last_alert_ts[alert_key] = (now, alert_net_profit_pct)
                return

        # Update state BEFORE sending (race condition protection)
        state.last_alert_ts[alert_key] = (now, alert_net_profit_pct)

        if (
            settings.fee_live_validation_enabled
            and state.gateway
            and alert_network
        ):
            try:
                from parsertang.fee_truth import fee_within_tolerance
                from parsertang.withdrawal_fees import (
                    fetch_withdraw_fee_live,
                    normalize_network_code,
                )

                base = symbol.split("/")[0] if "/" in symbol else ""
                net = normalize_network_code(alert_network, base)
                ex = None
                exchanges = getattr(state.gateway, "exchanges", None)
                if isinstance(exchanges, dict):
                    ex = exchanges.get(buy_exchange)

                fee_live = None
                fee_src = "error"
                if ex is not None and base and net:
                    fee_live, fee_src = fetch_withdraw_fee_live(
                        ex,
                        currency=base,
                        network=net,
                    )

                if fee_live is None:
                    state.funnel_counters["alerts_fee_live_blocked"] += 1
                    logger.info(
                        "ALERT SUPPRESSED | %s buy=%s sell=%s reason=fee_live_unavailable network=%s",
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        net or (alert_network or "N/A"),
                    )
                    if settings.telegram_send_suppressed_alerts_to_tech and hasattr(
                        state.alert_service, "send_tech"
                    ):
                        try:
                            loop.create_task(
                                state.alert_service.send_tech(
                                    f"ALERT SUPPRESSED | {symbol} {buy_exchange}→{sell_exchange} reason=fee_live_unavailable net={net_profit_pct:.3f}%"
                                )
                            )
                        except Exception:
                            pass
                    return

                if not fee_within_tolerance(
                    expected_fee_base=float(alert_withdraw_fee_base),
                    actual_fee_base=float(fee_live),
                    tolerance_pct=float(settings.fee_live_validation_tolerance_pct),
                    tolerance_base=float(settings.fee_live_validation_tolerance_base),
                ):
                    state.funnel_counters["alerts_fee_live_blocked"] += 1
                    logger.info(
                        "ALERT SUPPRESSED | %s buy=%s sell=%s reason=fee_live_mismatch network=%s ws_fee_base=%.12f live_fee_base=%.12f source=%s",
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        net or (alert_network or "N/A"),
                        float(alert_withdraw_fee_base),
                        float(fee_live),
                        fee_src,
                    )
                    if settings.telegram_send_suppressed_alerts_to_tech and hasattr(
                        state.alert_service, "send_tech"
                    ):
                        try:
                            loop.create_task(
                                state.alert_service.send_tech(
                                    f"ALERT SUPPRESSED | {symbol} {buy_exchange}→{sell_exchange} reason=fee_live_mismatch net={net_profit_pct:.3f}%"
                                )
                            )
                        except Exception:
                            pass
                    return
            except Exception:
                pass

        if settings.alert_evidence_enabled or settings.alert_verify_enabled:
            try:
                from parsertang.alert_evidence import append_jsonl
                from parsertang.alert_truth import AlertTruthEvidence, fee_cache_age_seconds

                buy_ts = float(buy_snapshot.get("ts", 0.0) or 0.0)
                sell_ts = float(sell_snapshot.get("ts", 0.0) or 0.0)
                age_buy_ms = int((now_wall - buy_ts) * 1000) if buy_ts > 0 else -1
                age_sell_ms = int((now_wall - sell_ts) * 1000) if sell_ts > 0 else -1
                skew_ms = int(abs(buy_ts - sell_ts) * 1000) if buy_ts and sell_ts else -1
                fee_age = fee_cache_age_seconds(now_wall, fee_manager=state.fee_manager)
                evidence = AlertTruthEvidence(
                    ts_wall=now_wall,
                    symbol=symbol,
                    buy_ex=buy_exchange,
                    sell_ex=sell_exchange,
                    ws_buy=best_ask,
                    ws_sell=best_bid,
                    ws_ts_buy=buy_ts,
                    ws_ts_sell=sell_ts,
                    ws_age_buy_ms=age_buy_ms,
                    ws_age_sell_ms=age_sell_ms,
                    ws_skew_ms=skew_ms,
                    net_profit_pct=alert_net_profit_pct,
                    network=alert_network,
                    trade_fees_pct=alert_trade_fees_pct,
                    withdraw_fee_pct=alert_withdraw_fee_pct,
                    withdraw_fee_base=alert_withdraw_fee_base,
                    fee_cache_age_seconds=fee_age,
                    rest_buy=getattr(v2_validation, "rest_buy", None) if v2_validation else None,
                    rest_sell=getattr(v2_validation, "rest_sell", None) if v2_validation else None,
                )
                if settings.alert_evidence_enabled:
                    append_jsonl(settings.alert_evidence_path, evidence.to_record())

                if settings.alert_verify_enabled and state.gateway:
                    from parsertang.alert_truth import verify_alert_truth_after_delay

                    validator = Validator(state.gateway)
                    loop.create_task(
                        verify_alert_truth_after_delay(validator=validator, evidence=evidence)
                    )
            except Exception:
                pass

        try:
            message = format_alert(
                symbol=symbol,
                buy_exchange=buy_exchange,
                buy_price=alert_best_ask,
                sell_exchange=sell_exchange,
                sell_price=alert_best_bid,
                gross_spread_pct=((alert_best_bid - alert_best_ask) / alert_best_ask) * 100.0 if alert_best_ask else 0.0,
                trade_fees_pct=alert_trade_fees_pct,
                withdraw_fee_pct=alert_withdraw_fee_pct,
                net_profit_pct=alert_net_profit_pct,
                net_profit_transfer_pct=alert_net_profit_transfer_pct,
                net_profit_funded_pct=alert_net_profit_funded_pct,
                bid_liq_usd=sell_snapshot.get("bid_liq_usd", 0.0),
                ask_liq_usd=buy_snapshot.get("ask_liq_usd", 0.0),
                network=(alert_network or "N/A"),
                withdrawal_fee_base=alert_withdraw_fee_base,
                fee_confidence=alert_fee_confidence,
            )
        except Exception:
            state.funnel_counters["alerts_error"] += 1
            logger.exception(
                "ALERT ERROR | %s buy=%s sell=%s net=%.3f%%",
                symbol,
                buy_exchange,
                sell_exchange,
                alert_net_profit_pct,
            )
            # Roll back dedup state: nothing was actually sent.
            state.last_alert_ts.pop(alert_key, None)
            return
        logger.info(
            "ALERT SENT | %s buy=%s sell=%s net=%.3f%%",
            symbol,
            buy_exchange,
            sell_exchange,
            alert_net_profit_pct,
        )
        state.funnel_counters["alerts_sent"] += 1
        channel = pick_alert_channel(alert_net_profit_pct)
        logger.info(
            "ALERT ROUTE | %s buy=%s sell=%s channel=%s net=%.3f%%",
            symbol,
            buy_exchange,
            sell_exchange,
            channel,
            alert_net_profit_pct,
        )
        if channel == "trade":
            state.alert_service.send(message)
        else:
            try:
                loop.create_task(state.alert_service.send_tech(message))
            except Exception:
                # Strict: do not fall back to trader chat if tech routing fails.
                pass

    # Start trading cycle if trader is enabled
    if state.trader:
        # Create Opportunity object for trader
        opportunity = Opportunity(
            symbol=symbol,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            buy_price=best_ask,
            sell_price=best_bid,
            gross_spread_pct=gross_spread_pct,
            trade_fees_pct=trade_fees_pct,
            withdraw_fee_pct=withdraw_fee_pct,
            net_profit_pct=net_profit_pct,
            bid_liq_usd=sell_snapshot.get("bid_liq_usd", 0.0),
            ask_liq_usd=buy_snapshot.get("ask_liq_usd", 0.0),
            network=network,
            withdrawal_fee_base=withdraw_fee_base,
            buy_taker_fee_pct=buy_fee_pct,
            sell_taker_fee_pct=sell_fee_pct,
            withdraw_from_exchange=buy_exchange,
        )

        # ASYNC BOUNDARY FIX: Use injected event loop to create task
        # This avoids RuntimeError when called from sync context (WS callback)
        task = loop.create_task(state.trader.start_cycle(opportunity))

        # Add error handler to catch exceptions in task
        task.add_done_callback(
            lambda t: (
                logger.error(
                    "trader.start_cycle failed: %s",
                    t.exception(),
                    exc_info=t.exception(),
                )
                if not t.cancelled() and t.exception()
                else None
            )
        )
