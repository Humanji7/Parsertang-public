"""Cross-exchange symbol selection for maximum arbitrage overlap.

This module implements a two-phase allocation algorithm that prioritizes symbols
available on multiple exchanges, maximizing arbitrage detection opportunities.

Reference: Implementation Plan - Maximize Symbol Overlap Between Exchanges
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from parsertang.config import EXCLUDED_BASE_ASSETS, get_exchange_symbol_limits
from parsertang.exchanges import ExchangeGateway

logger = logging.getLogger(__name__)


@dataclass
class SymbolMeta:
    """Metadata for a trading symbol across exchanges.

    Attributes:
        symbol: Trading pair symbol (e.g., "XRP/USDT")
        exchanges: Mapping of exchange_id to 24h quote volume
    """

    symbol: str
    exchanges: dict[str, float] = field(default_factory=dict)

    @property
    def exchange_count(self) -> int:
        """Number of exchanges where this symbol is available."""
        return len(self.exchanges)

    @property
    def aggregate_volume(self) -> float:
        """Total volume across all exchanges."""
        return sum(self.exchanges.values())


def build_symbol_index(markets: dict[str, dict]) -> dict[str, SymbolMeta]:
    """Build index of all stable-quote symbols across exchanges.

    Filters symbols to include only:
    - Stable quote currencies (USDT, USDC, DAI, etc.)
    - Excludes blacklisted base assets (BTC, ETH, SOL)

    Args:
        markets: Dict of exchange_id -> market data from load_markets()

    Returns:
        Dict of symbol -> SymbolMeta with volume per exchange
    """
    index: dict[str, SymbolMeta] = {}

    for ex_id, market_data in markets.items():
        if not market_data:
            logger.warning(
                "SYMBOLS | exchange=%s returned empty markets, excluding from overlap",
                ex_id,
            )
            continue

        for symbol, info in market_data.items():
            # Filter: stable quote only
            if not ExchangeGateway.is_stable_quote(symbol):
                continue

            # Filter: spot markets only (avoid swaps/futures like "ADA/USDT:USDT")
            if isinstance(info, dict):
                if info.get("spot") is True:
                    pass
                elif info.get("swap") is True or info.get("future") is True:
                    continue
                else:
                    # Some exchanges don't set spot/swap flags consistently.
                    # Heuristic: derivatives often contain ':' suffix.
                    if ":" in symbol:
                        continue
            else:
                if ":" in symbol:
                    continue

            # Filter: skip inactive symbols (OKX returns symbols that exist but aren't tradeable)
            if isinstance(info, dict) and info.get("active") is False:
                continue

            # Filter: exclude blacklisted base assets
            try:
                base, _ = symbol.split("/")
                if base.upper() in EXCLUDED_BASE_ASSETS:
                    continue
            except ValueError:
                continue

            # Extract volume from market info
            volume = 0.0
            if isinstance(info, dict):
                info_data = info.get("info", {})
                if isinstance(info_data, dict):
                    volume = float(
                        info_data.get("quoteVolume", 0)
                        or info_data.get("volume", 0)
                        or 0
                    )

            # Add to index
            if symbol not in index:
                index[symbol] = SymbolMeta(symbol=symbol)
            index[symbol].exchanges[ex_id] = volume

    logger.info(
        "SYMBOLS | built index with %d unique symbols from %d exchanges",
        len(index),
        len([m for m in markets.values() if m]),
    )

    return index


def rank_symbols_for_overlap(symbol_index: dict[str, SymbolMeta]) -> list[SymbolMeta]:
    """Rank symbols by cross-exchange availability.

    Sorting criteria (descending priority):
    1. Number of exchanges (more = better for arbitrage)
    2. Aggregate volume (higher liquidity = safer trades)
    3. Symbol name (deterministic tie-breaker)

    Args:
        symbol_index: Dict from build_symbol_index()

    Returns:
        List of SymbolMeta sorted by overlap priority
    """
    return sorted(
        symbol_index.values(),
        key=lambda m: (-m.exchange_count, -m.aggregate_volume, m.symbol),
    )


def diversify_ranked_symbols(
    ranked_symbols: list[SymbolMeta],
    *,
    target_unique: int,
    diversify_fraction: float,
    pool_multiplier: int = 5,
) -> list[SymbolMeta]:
    """Inject mid-ranked multi-ex symbols into the head of the list.

    Motivation: top-by-volume symbols are often the most efficient and may produce
    few real arbitrage candidates. To discover opportunities, we intentionally
    mix in mid-cap overlaps while keeping deterministic behavior.

    This function preserves ordering stability and never drops symbols; it only
    reorders the start of the multi-ex segment.
    """
    if not ranked_symbols:
        return ranked_symbols
    if target_unique <= 0:
        return ranked_symbols
    if diversify_fraction <= 0:
        return ranked_symbols

    frac = float(diversify_fraction)
    if frac >= 1.0:
        frac = 0.99

    multi = [m for m in ranked_symbols if m.exchange_count >= 2]
    single = [m for m in ranked_symbols if m.exchange_count < 2]
    if not multi:
        return ranked_symbols

    target = int(target_unique)
    top_take = max(1, int(round(target * (1.0 - frac))))
    mid_take = max(0, target - top_take)
    if mid_take == 0:
        return ranked_symbols

    mid_pool_size = max(mid_take, mid_take * max(1, int(pool_multiplier)))
    mid_pool = multi[top_take : top_take + mid_pool_size]
    if not mid_pool:
        return ranked_symbols

    # Deterministically sample across the pool (even spacing).
    step = max(1, len(mid_pool) // mid_take)
    selected_mid: list[SymbolMeta] = []
    used_symbols: set[str] = set()
    for i in range(mid_take):
        idx = i * step
        if idx >= len(mid_pool):
            break
        selected_mid.append(mid_pool[idx])
        used_symbols.add(mid_pool[idx].symbol)
    if len(selected_mid) < mid_take:
        for meta in mid_pool:
            if meta.symbol in used_symbols:
                continue
            selected_mid.append(meta)
            used_symbols.add(meta.symbol)
            if len(selected_mid) >= mid_take:
                break

    mixed_head: list[SymbolMeta] = []
    used_head: set[str] = set()

    def _add(meta: SymbolMeta) -> None:
        if meta.symbol in used_head:
            return
        mixed_head.append(meta)
        used_head.add(meta.symbol)

    top_iter = iter(multi[:top_take])
    mid_iter = iter(selected_mid)

    # Interleave to keep both high-volume and exploratory symbols early.
    while len(mixed_head) < target:
        progressed = False
        try:
            _add(next(top_iter))
            progressed = True
        except StopIteration:
            pass
        if len(mixed_head) >= target:
            break
        try:
            _add(next(mid_iter))
            progressed = True
        except StopIteration:
            pass
        if not progressed:
            break

    # Append the remainder in original order.
    remainder_multi = [m for m in multi if m.symbol not in used_head]
    return mixed_head + remainder_multi + single


def allocate_symbols_per_exchange(
    symbol_index: dict[str, SymbolMeta],
    ranked_symbols: list[SymbolMeta],
    max_per_exchange: int,
    exchange_limits: dict[str, int] | None = None,
    *,
    min_overlap_exchanges: int = 2,
) -> dict[str, list[str]]:
    """Allocate symbols to maximize cross-exchange overlap.

    Two-phase algorithm:
    1. Phase 1: Fill with multi-exchange symbols (exchange_count >= 2)
       - Add symbol to the best `min_overlap_exchanges` exchanges where it's available
       - Only add if at least `min_overlap_exchanges` exchanges still have capacity
    2. Phase 2: Fill remaining slots with single-exchange symbols
       - Sorted by volume (from ranked_symbols order)

    Args:
        symbol_index: Dict from build_symbol_index()
        ranked_symbols: List from rank_symbols_for_overlap()
        max_per_exchange: Maximum symbols per exchange

    Returns:
        Dict of exchange_id -> list of symbols to subscribe
    """
    if max_per_exchange <= 0:
        return {}

    if not ranked_symbols:
        return {}

    if min_overlap_exchanges < 2:
        raise ValueError("min_overlap_exchanges must be >= 2")

    result: dict[str, list[str]] = defaultdict(list)
    exchange_counts: dict[str, int] = defaultdict(int)

    # Collect all exchange IDs and determine per-exchange limits
    all_exchanges = set()
    for meta in symbol_index.values():
        all_exchanges.update(meta.exchanges.keys())

    # Per-exchange limits: use override if available, else global max
    limits = exchange_limits or {}
    ex_limits = {ex: limits.get(ex, max_per_exchange) for ex in all_exchanges}

    # Phase 1: Multi-exchange symbols
    multi_ex_added = 0
    for meta in ranked_symbols:
        if meta.exchange_count < 2:
            continue

        # IMPORTANT: do not require capacity on *every* exchange where the symbol exists.
        # Otherwise, a tight limit on one exchange (e.g., MEXC) can prevent adding the
        # symbol to other exchanges that still have capacity (reducing overlap and
        # arbitrage opportunities).
        available = [
            ex
            for ex in meta.exchanges
            if exchange_counts[ex] < ex_limits.get(ex, max_per_exchange)
        ]
        if len(available) < min_overlap_exchanges:
            continue

        # Prefer exchanges with higher limits/volume (more stable, higher throughput).
        available.sort(
            key=lambda ex: (
                -ex_limits.get(ex, max_per_exchange),
                -float(meta.exchanges.get(ex, 0.0) or 0.0),
                ex,
            )
        )

        # IMPORTANT: when a symbol exists on 3+ exchanges, subscribing on *all* of them
        # can cause a tight exchange limit (e.g., MEXC) to be filled entirely with
        # tri-overlap symbols. That reduces pairwise coverage and lowers alert throughput.
        #
        # We subscribe on exactly `min_overlap_exchanges` best exchanges for this symbol.
        for ex in available[:min_overlap_exchanges]:
            result[ex].append(meta.symbol)
            exchange_counts[ex] += 1
        multi_ex_added += 1

    # Phase 2: Fill remaining with single-exchange symbols
    single_ex_added = 0
    for meta in ranked_symbols:
        if meta.exchange_count != 1:
            continue

        ex = list(meta.exchanges.keys())[0]
        if exchange_counts[ex] < ex_limits.get(ex, max_per_exchange):
            result[ex].append(meta.symbol)
            exchange_counts[ex] += 1
            single_ex_added += 1

    # Log allocation stats
    for ex_id in sorted(all_exchanges):
        count = len(result.get(ex_id, []))
        limit = ex_limits.get(ex_id, max_per_exchange)
        logger.info(
            "SYMBOLS | %s allocated %d symbols (max=%d)",
            ex_id,
            count,
            limit,
        )

    logger.info(
        "SYMBOLS | allocation complete: multi_ex=%d single_ex=%d",
        multi_ex_added,
        single_ex_added,
    )

    # Monitoring: write the current per-exchange universe to disk so operators can
    # understand "which pairs we are looking at" without parsing logs.
    #
    # This is intentionally best-effort and must never affect runtime behavior.
    try:
        snapshot_path = Path("data/universe_symbols.json")
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "max_per_exchange": int(max_per_exchange),
            "exchange_limits": {k: int(v) for k, v in sorted(ex_limits.items())},
            "allocated_counts": {k: len(result.get(k, [])) for k in sorted(all_exchanges)},
            "multi_ex_added": int(multi_ex_added),
            "single_ex_added": int(single_ex_added),
            "symbols": {k: list(result.get(k, [])) for k in sorted(all_exchanges)},
        }
        snapshot_path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
        logger.info("SYMBOLS | wrote universe snapshot: %s", snapshot_path)
    except Exception as e:
        logger.warning("SYMBOLS | failed to write universe snapshot: %s", e)

    return dict(result)


def select_symbols(
    markets: dict[str, dict],
    max_per_exchange: int,
    strategy: Literal["local_volume", "cross_exchange"] = "cross_exchange",
) -> dict[str, list[str]]:
    """High-level symbol selection dispatcher.

    Args:
        markets: Dict of exchange_id -> market data
        max_per_exchange: Maximum symbols per exchange
        strategy: Selection strategy

    Returns:
        Dict of exchange_id -> list of symbols
    """
    if strategy == "cross_exchange":
        symbol_index = build_symbol_index(markets)
        ranked = rank_symbols_for_overlap(symbol_index)

        # Log metrics
        multi_ex_count = sum(1 for m in symbol_index.values() if m.exchange_count >= 2)
        logger.info(
            "SYMBOLS | strategy=cross_exchange total_candidates=%d multi_ex_symbols=%d",
            len(symbol_index),
            multi_ex_count,
        )

        return allocate_symbols_per_exchange(
            symbol_index, ranked, max_per_exchange, get_exchange_symbol_limits()
        )

    # Fallback: local_volume (not implemented here, use existing main.py logic)
    logger.info("SYMBOLS | strategy=local_volume (fallback)")
    return {}


def select_symbols_core_periphery(
    markets: dict[str, dict],
    max_per_exchange: int,
    core_exchanges: list[str],
    periphery_exchanges: list[str],
    exchange_limits: dict[str, int] | None = None,
) -> dict[str, list[str]]:
    """Select symbols using core+periphery strategy.

    Core exchanges are allocated first to maximize overlap within the core set.
    Periphery exchanges then prioritize symbols already selected in core.
    """
    if max_per_exchange <= 0:
        return {}

    if not markets:
        return {}

    logger.info(
        "SYMBOLS | strategy=core_periphery core=%s periphery=%s",
        ",".join(core_exchanges) if core_exchanges else "-",
        ",".join(periphery_exchanges) if periphery_exchanges else "-",
    )

    limits = exchange_limits or {}
    core_markets = {
        ex: markets[ex] for ex in core_exchanges if ex in markets and markets[ex]
    }

    # Core allocation
    core_index = build_symbol_index(core_markets)
    core_ranked = rank_symbols_for_overlap(core_index)
    core_min_overlap = max(2, len(core_exchanges)) if core_exchanges else 2
    core_alloc = allocate_symbols_per_exchange(
        core_index,
        core_ranked,
        max_per_exchange,
        limits,
        min_overlap_exchanges=core_min_overlap,
    )

    # Ordered list of selected core symbols (deterministic)
    core_selected = {sym for ex_syms in core_alloc.values() for sym in ex_syms}
    core_symbols_ordered = [
        meta.symbol for meta in core_ranked if meta.symbol in core_selected
    ]

    result: dict[str, list[str]] = {**core_alloc}

    # Periphery allocation prioritizes core symbols first
    for ex in periphery_exchanges:
        ex_markets = markets.get(ex) or {}
        if not ex_markets:
            result[ex] = []
            continue

        limit = limits.get(ex, max_per_exchange)
        selected: list[str] = []

        # First pass: core symbols available on this exchange
        for symbol in core_symbols_ordered:
            if symbol in ex_markets and len(selected) < limit:
                selected.append(symbol)

        # Second pass: fill remaining with stable symbols (ranked by volume)
        if len(selected) < limit:
            if not selected:
                logger.info(
                    "SYMBOLS | periphery fallback ex=%s no core overlap; selecting local symbols",
                    ex,
                )
            per_index = build_symbol_index({ex: ex_markets})
            per_ranked = rank_symbols_for_overlap(per_index)
            for meta in per_ranked:
                if meta.symbol in selected:
                    continue
                selected.append(meta.symbol)
                if len(selected) >= limit:
                    break

        result[ex] = selected

    return result
