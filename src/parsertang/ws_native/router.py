import time
from dataclasses import dataclass
from parsertang.liquidity import liquidity_usd_within_window
from parsertang.slippage import estimate_buy_slippage_pct, estimate_sell_slippage_pct


@dataclass(frozen=True)
class RouterSnapshot:
    best_bid: float
    best_ask: float
    bid_liq_usd: float
    ask_liq_usd: float
    bid_slip_pct: float
    ask_slip_pct: float
    ts: float


def build_snapshot(ev, depth, liquidity_window_pct: float, trade_volume_usd: float):
    bids = depth.bids
    asks = depth.asks
    bid_liq, ask_liq = liquidity_usd_within_window(bids, asks, liquidity_window_pct)
    ask_slip = estimate_buy_slippage_pct(asks, trade_volume_usd)
    bid_slip = estimate_sell_slippage_pct(bids, trade_volume_usd)
    return RouterSnapshot(
        best_bid=ev.bid,
        best_ask=ev.ask,
        bid_liq_usd=bid_liq,
        ask_liq_usd=ask_liq,
        bid_slip_pct=bid_slip,
        ask_slip_pct=ask_slip,
        ts=time.time(),
    )
