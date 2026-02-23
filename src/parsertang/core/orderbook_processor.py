"""Orderbook processing and WebSocket callback handlers.

Handles:
- WebSocket orderbook callbacks (on_orderbook_update)
- Orderbook validation and transformation
- Liquidity and slippage calculations
- State updates (via AppState)
- WS health metrics tracking

Thread Safety:
- All state mutations go through AppState with proper locking
- Called from async WebSocket callbacks — no blocking operations
- Periodic cleanup tasks run independently

Workflow:
1. WS callback receives raw orderbook → on_orderbook_update()
2. Validate and transform bids/asks
3. Calculate liquidity and slippage
4. Update AppState.orderbooks with snapshot
5. Trigger arbitrage evaluation if liquidity passes threshold
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Callable, List, Tuple

if TYPE_CHECKING:
    from parsertang.core.state_manager import AppState

logger = logging.getLogger(__name__)


def parse_orderbook_entries(
    entries: list,
) -> List[Tuple[float, float]]:
    """Parse and validate orderbook entries (bids or asks).

    Converts raw exchange data to (price, amount) tuples with strict validation.
    Fail Fast: Skips invalid entries (malformed data), but doesn't crash.

    Args:
        entries: Raw orderbook entries from exchange (list of [price, amount] arrays)

    Returns:
        List of validated (price, amount) tuples
    """
    result: List[Tuple[float, float]] = []
    for entry in entries:
        try:
            price = float(entry[0])
            amount = float(entry[1])
            result.append((price, amount))
        except (TypeError, ValueError, IndexError):
            # Skip malformed entry (Fail Fast: don't crash on bad data)
            continue
    return result


def on_orderbook_update(
    ex_id: str,
    symbol: str,
    ob: dict,
    state: AppState,
    settings: dict,
    liquidity_fn: Callable,
    slippage_buy_fn: Callable,
    slippage_sell_fn: Callable,
    evaluate_arbitrage_fn: Callable[[str], None],
    update_ws_metrics: bool = True,
) -> None:
    """Process orderbook update from WebSocket and update state.

    Called by WebSocket callback at high frequency (~100-1000 Hz per exchange).
    Keep logic minimal — heavy computation should be in evaluate_arbitrage.

    Args:
        ex_id: Exchange ID (e.g., "bybit", "gate")
        symbol: Trading pair (e.g., "BTC/USDT")
        ob: Raw orderbook dict with "bids" and "asks" keys
        state: AppState instance for thread-safe state updates
        settings: Settings dict with liquidity_window_pct, liquidity_usd_threshold, trade_volume_usd
        liquidity_fn: Function to calculate liquidity (bids, asks, window_pct) -> (bid_liq_usd, ask_liq_usd)
        slippage_buy_fn: Function to estimate buy slippage (asks, volume_usd) -> slip_pct
        slippage_sell_fn: Function to estimate sell slippage (bids, volume_usd) -> slip_pct
        evaluate_arbitrage_fn: Function to trigger arbitrage evaluation for symbol

    Side Effects:
        - Updates WS health metrics (state.ws_metrics)
        - Updates orderbook state (state.orderbooks)
        - Increments liquidity stats (state.stats)
        - Triggers periodic cleanup (every 30s)
        - Calls evaluate_arbitrage_fn if liquidity passes threshold
    """
    from collections import defaultdict

    from parsertang.core.state_manager import OrderbookSnapshot

    # Track WS health metrics (no lock needed — metrics_lock used during read+reset)
    if update_ws_metrics:
        state.ws_metrics.update_counters[ex_id] += 1
        state.ws_metrics.symbols_seen[ex_id].add(symbol)

    # Extract bids/asks
    bids_src = ob.get("bids") or []
    asks_src = ob.get("asks") or []

    if not bids_src or not asks_src:
        # Empty orderbook — skip (Fail Fast: invalid data)
        return

    # Parse and validate entries
    bids = parse_orderbook_entries(bids_src)
    asks = parse_orderbook_entries(asks_src)

    if not bids or not asks:
        # No valid entries after parsing — skip
        return

    # Extract best bid/ask (already sorted by exchange)
    best_bid = bids[0][0]
    best_ask = asks[0][0]

    # Calculate liquidity within configured window
    bid_liq_usd, ask_liq_usd = liquidity_fn(
        bids, asks, settings["liquidity_window_pct"]
    )

    # Precompute slippage estimates for configured trade size
    ask_slip_pct = slippage_buy_fn(asks, settings["trade_volume_usd"])
    bid_slip_pct = slippage_sell_fn(bids, settings["trade_volume_usd"])

    # Create immutable snapshot
    snapshot = OrderbookSnapshot(
        best_bid=best_bid,
        best_ask=best_ask,
        bid_liq_usd=bid_liq_usd,
        ask_liq_usd=ask_liq_usd,
        bid_slip_pct=bid_slip_pct,
        ask_slip_pct=ask_slip_pct,
        ts=time.time(),
    )

    # Update state (thread-safe via AppState.update_orderbook)
    # NOTE: This is sync code called from async context, but update_orderbook is async.
    # We need to schedule it properly. For now, we'll use a synchronous approach
    # by storing directly (accepting the race condition risk for performance).
    # Alternative: Make on_orderbook_update async and await the update.
    # FIXME: This direct assignment bypasses the lock! Need to refactor caller to be async.
    state.orderbooks[(ex_id, symbol)] = snapshot

    # Memory cleanup: remove stale entries every 30 seconds (TTL 120s)
    now_cleanup = time.time()
    if now_cleanup - state.state_last_cleanup > 30:
        state.state_last_cleanup = now_cleanup
        stale_keys = [
            k for k, v in list(state.orderbooks.items()) if now_cleanup - v.ts > 120
        ]
        for k in stale_keys:
            del state.orderbooks[k]
        if stale_keys:
            logger.debug("STATE CLEANUP | Removed %d stale entries", len(stale_keys))

    # Early exit: liquidity filter (before incrementing stats or logging)
    if (
        bid_liq_usd < settings["liquidity_usd_threshold"]
        or ask_liq_usd < settings["liquidity_usd_threshold"]
    ):
        return

    # Liquidity passed — log and update metrics
    logger.info(
        "LIQ OK | %s %s bid=%.2f ask=%.2f bid_liq=%.2f ask_liq=%.2f",
        ex_id,
        symbol,
        best_bid,
        best_ask,
        bid_liq_usd,
        ask_liq_usd,
    )

    # Update metrics (no lock needed — metrics_lock used during read+reset)
    # Access via defaultdict pattern to avoid AttributeError
    if not hasattr(state, "funnel_counters"):
        state.funnel_counters = defaultdict(int)
    state.funnel_counters["liq_ok"] += 1

    if not hasattr(state, "stats"):
        state.stats = defaultdict(int)
    state.stats[ex_id] += 1

    # Trigger arbitrage evaluation (synchronous call)
    evaluate_arbitrage_fn(symbol)


async def on_orderbook_update_async(
    ex_id: str,
    symbol: str,
    ob: dict,
    state: AppState,
    settings: dict,
    liquidity_fn: Callable,
    slippage_buy_fn: Callable,
    slippage_sell_fn: Callable,
    evaluate_arbitrage_fn: Callable[[str], None],
) -> None:
    """Async wrapper for orderbook update with proper state locking.

    Use this version when calling from async context (e.g., WebSocket callback).
    Properly awaits AppState.update_orderbook() for thread safety.

    Args:
        Same as on_orderbook_update()

    Side Effects:
        Same as on_orderbook_update(), but with proper async locking
    """
    from collections import defaultdict

    from parsertang.core.state_manager import OrderbookSnapshot

    # Track WS health metrics (no lock needed — metrics_lock used during read+reset)
    state.ws_metrics.update_counters[ex_id] += 1
    state.ws_metrics.symbols_seen[ex_id].add(symbol)

    # Extract bids/asks
    bids_src = ob.get("bids") or []
    asks_src = ob.get("asks") or []

    if not bids_src or not asks_src:
        return

    # Parse and validate entries
    bids = parse_orderbook_entries(bids_src)
    asks = parse_orderbook_entries(asks_src)

    if not bids or not asks:
        return

    # Extract best bid/ask
    best_bid = bids[0][0]
    best_ask = asks[0][0]

    # Calculate liquidity and slippage
    bid_liq_usd, ask_liq_usd = liquidity_fn(
        bids, asks, settings["liquidity_window_pct"]
    )
    ask_slip_pct = slippage_buy_fn(asks, settings["trade_volume_usd"])
    bid_slip_pct = slippage_sell_fn(bids, settings["trade_volume_usd"])

    # Create snapshot
    snapshot = OrderbookSnapshot(
        best_bid=best_bid,
        best_ask=best_ask,
        bid_liq_usd=bid_liq_usd,
        ask_liq_usd=ask_liq_usd,
        bid_slip_pct=bid_slip_pct,
        ask_slip_pct=ask_slip_pct,
        ts=time.time(),
    )

    # Update state with proper locking
    await state.update_orderbook(ex_id, symbol, snapshot)

    # Memory cleanup (async-safe)
    now_cleanup = time.time()
    if now_cleanup - state.state_last_cleanup > 30:
        state.state_last_cleanup = now_cleanup
        removed = await state.cleanup_stale_orderbooks(ttl_seconds=120)
        if removed:
            logger.debug("STATE CLEANUP | Removed %d stale entries", removed)

    # Early exit: liquidity filter
    if (
        bid_liq_usd < settings["liquidity_usd_threshold"]
        or ask_liq_usd < settings["liquidity_usd_threshold"]
    ):
        return

    # Liquidity passed
    logger.info(
        "LIQ OK | %s %s bid=%.2f ask=%.2f bid_liq=%.2f ask_liq=%.2f",
        ex_id,
        symbol,
        best_bid,
        best_ask,
        bid_liq_usd,
        ask_liq_usd,
    )

    # Update metrics (async-safe via lock in caller or accept race condition for counters)
    if not hasattr(state, "funnel_counters"):
        state.funnel_counters = defaultdict(int)
    state.funnel_counters["liq_ok"] += 1

    if not hasattr(state, "stats"):
        state.stats = defaultdict(int)
    state.stats[ex_id] += 1

    # Trigger arbitrage evaluation
    evaluate_arbitrage_fn(symbol)
