from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Quote:
    exchange: str
    symbol: str
    bid: float
    ask: float
    bid_liq_usd: float
    ask_liq_usd: float


@dataclass
class Opportunity:
    symbol: str
    buy_exchange: str
    buy_price: float
    sell_exchange: str
    sell_price: float
    gross_spread_pct: float
    trade_fees_pct: float  # Keep for backward compatibility (sum of buy + sell)
    withdraw_fee_pct: float
    net_profit_pct: float
    bid_liq_usd: float
    ask_liq_usd: float
    network: Optional[str]
    withdrawal_fee_base: float = (
        0.0  # Withdrawal fee in BASE CURRENCY (e.g., 0.0069 LTC)
    )
    buy_taker_fee_pct: float = 0.0  # Individual buy exchange taker fee
    sell_taker_fee_pct: float = 0.0  # Individual sell exchange taker fee
    withdraw_from_exchange: str = (
        ""  # Exchange to withdraw from (typically buy_exchange)
    )


def compute_gross_spread_pct(min_ask: float, max_bid: float) -> float:
    if min_ask <= 0:
        return 0.0
    return ((max_bid - min_ask) / min_ask) * 100.0


def compute_net_profit_pct(
    gross_spread_pct: float,
    buy_taker_fee_pct: float,
    sell_taker_fee_pct: float,
    withdrawal_fee_usd: float,
    trade_volume_usd: float,
) -> tuple[float, float, float]:
    """
    Compute net profit percentage using multiplicative fee compounding.

    Correct arbitrage profit calculation:
    1. Buy $100 → Pay buy_fee → Receive coins worth $(100 * (1 - buy_fee/100))
    2. Coins appreciate by gross_spread → Value = previous * (1 + gross/100)
    3. Withdraw → Pay withdraw_fee → Value = previous * (1 - withdraw_fee/100)
    4. Sell → Pay sell_fee → Receive $(previous * (1 - sell_fee/100))

    Final multiplier = (1 + gross/100) * (1 - buy_fee/100) * (1 - withdraw_fee/100) * (1 - sell_fee/100)

    Args:
        gross_spread_pct: Price difference percentage between exchanges
        buy_taker_fee_pct: Buy exchange taker fee percentage
        sell_taker_fee_pct: Sell exchange taker fee percentage
        withdrawal_fee_usd: Withdrawal fee in USD (converted to % of volume)
        trade_volume_usd: Trade volume in USD

    Returns:
        Tuple of (net_profit_pct, trade_fees_pct, withdraw_fee_pct)
    """
    # Convert fees to decimal multipliers for numerical stability
    withdraw_fee_pct = (withdrawal_fee_usd / trade_volume_usd) * 100.0
    trade_fees_pct = (
        buy_taker_fee_pct + sell_taker_fee_pct
    )  # Keep for backward compatibility

    # Multiplicative compounding formula
    # Each operation applies sequentially on remaining capital
    gross_multiplier = 1.0 + (gross_spread_pct / 100.0)
    buy_fee_multiplier = 1.0 - (buy_taker_fee_pct / 100.0)
    withdraw_fee_multiplier = 1.0 - (withdraw_fee_pct / 100.0)
    sell_fee_multiplier = 1.0 - (sell_taker_fee_pct / 100.0)

    # Total capital multiplier after all operations
    final_multiplier = (
        gross_multiplier
        * buy_fee_multiplier
        * withdraw_fee_multiplier
        * sell_fee_multiplier
    )

    # Convert back to percentage
    net_profit_pct = (final_multiplier - 1.0) * 100.0

    return net_profit_pct, trade_fees_pct, withdraw_fee_pct


def find_best_opportunity(symbol: str, quotes: List[Quote]) -> Optional[Opportunity]:
    if not quotes:
        return None
    # best buy (min ask) and best sell (max bid)
    best_buy = min(quotes, key=lambda q: q.ask)
    best_sell = max(quotes, key=lambda q: q.bid)
    gross = compute_gross_spread_pct(best_buy.ask, best_sell.bid)
    # Fees are plugged later from per-exchange config
    return Opportunity(
        symbol=symbol,
        buy_exchange=best_buy.exchange,
        buy_price=best_buy.ask,
        sell_exchange=best_sell.exchange,
        sell_price=best_sell.bid,
        gross_spread_pct=gross,
        trade_fees_pct=0.0,
        withdraw_fee_pct=0.0,
        net_profit_pct=gross,
        bid_liq_usd=best_sell.bid_liq_usd,
        ask_liq_usd=best_buy.ask_liq_usd,
        network=None,
    )
