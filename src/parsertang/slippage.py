from __future__ import annotations

from typing import Iterable, Tuple


def compute_vwap_buy_for_usd(
    asks: Iterable[Tuple[float, float]], target_usd: float
) -> float | None:
    """Return VWAP price for buying up to target_usd using asks.

    Args:
        asks: Iterable of (price, amount_base), best ask first.
        target_usd: Desired notional in USD.

    Returns:
        VWAP buy price if fully filled, else None.
    """
    if target_usd <= 0:
        return None

    spent_usd = 0.0
    bought_base = 0.0

    for price, amount_base in asks:
        if price <= 0 or amount_base <= 0:
            continue
        remaining_usd = target_usd - spent_usd
        if remaining_usd <= 0:
            break

        level_usd = price * amount_base
        take_usd = min(level_usd, remaining_usd)
        spent_usd += take_usd
        bought_base += take_usd / price

        if spent_usd >= target_usd:
            break

    if spent_usd + 1e-9 < target_usd or bought_base <= 0:
        return None

    return spent_usd / bought_base


def compute_vwap_sell_for_base(
    bids: Iterable[Tuple[float, float]], target_base: float
) -> float | None:
    """Return VWAP price for selling target_base into bids.

    Args:
        bids: Iterable of (price, amount_base), best bid first.
        target_base: Desired amount in base currency.

    Returns:
        VWAP sell price if fully filled, else None.
    """
    if target_base <= 0:
        return None

    sold_base = 0.0
    received_usd = 0.0

    for price, amount_base in bids:
        if price <= 0 or amount_base <= 0:
            continue
        remaining_base = target_base - sold_base
        if remaining_base <= 0:
            break

        take_base = min(amount_base, remaining_base)
        sold_base += take_base
        received_usd += take_base * price

        if sold_base >= target_base:
            break

    if sold_base + 1e-12 < target_base or received_usd <= 0:
        return None

    return received_usd / sold_base


def estimate_buy_slippage_pct(
    asks: list[tuple[float, float]], target_usd: float
) -> float:
    """Estimate buy-side slippage % for spending target_usd."""
    if not asks:
        return float("inf")
    best_ask = asks[0][0]
    if best_ask <= 0:
        return float("inf")
    vwap = compute_vwap_buy_for_usd(asks, target_usd)
    if vwap is None:
        return float("inf")
    return (vwap / best_ask - 1.0) * 100.0


def estimate_sell_slippage_pct(
    bids: list[tuple[float, float]], target_usd: float
) -> float:
    """Estimate sell-side slippage % for selling base worth target_usd at best bid."""
    if not bids:
        return float("inf")
    best_bid = bids[0][0]
    if best_bid <= 0:
        return float("inf")
    target_base = target_usd / best_bid
    vwap = compute_vwap_sell_for_base(bids, target_base)
    if vwap is None:
        return float("inf")
    return (1.0 - vwap / best_bid) * 100.0
