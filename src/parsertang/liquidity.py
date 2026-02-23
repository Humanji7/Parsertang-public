from __future__ import annotations

from typing import Sequence, Tuple


def compute_mid(best_bid: float, best_ask: float) -> float:
    return (best_bid + best_ask) / 2.0


def liquidity_usd_within_window(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
    window_pct: float,
) -> Tuple[float, float]:
    if not bids or not asks:
        return 0.0, 0.0
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = compute_mid(best_bid, best_ask)
    bid_min = mid * (1 - window_pct / 100.0)
    ask_max = mid * (1 + window_pct / 100.0)

    bid_usd = 0.0
    for entry in bids:
        if len(entry) < 2:
            continue
        price, amount = entry[0], entry[1]
        if price < bid_min:
            break
        bid_usd += price * amount

    ask_usd = 0.0
    for entry in asks:
        if len(entry) < 2:
            continue
        price, amount = entry[0], entry[1]
        if price > ask_max:
            break
        ask_usd += price * amount

    return bid_usd, ask_usd
