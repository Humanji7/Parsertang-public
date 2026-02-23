"""Fee conversion utilities.

This module provides utility functions for converting withdrawal fees
between USD and base currency coins. These conversions are critical for
accurate fee accounting in arbitrage trading cycles.
"""

from __future__ import annotations

import logging

from parsertang.exchanges import STABLE_QUOTES

logger = logging.getLogger(__name__)


def convert_usd_fee_to_base_coins(
    fee_usd: float, base_currency: str, base_price_usd: float
) -> float:
    """Convert USD withdrawal fee to base currency coins.

    This function is used to convert withdrawal fees from their USD value
    (as fetched from exchange APIs) to the actual number of coins that
    will be deducted during withdrawal.

    Args:
        fee_usd: Withdrawal fee in USD (from exchange API)
        base_currency: Base currency symbol (e.g., "USDT", "LTC", "BTC")
        base_price_usd: Current price of base currency in USD

    Returns:
        Withdrawal fee in base currency coins

    Examples:
        >>> # USDT: $1 fee on USDT (stablecoin, 1:1 with USD)
        >>> convert_usd_fee_to_base_coins(1.0, "USDT", 1.0)
        1.0  # 1 USDT

        >>> # LTC: $5 fee on LTC at $100/LTC
        >>> convert_usd_fee_to_base_coins(5.0, "LTC", 100.0)
        0.05  # 0.05 LTC

        >>> # BTC: $10 fee on BTC at $50,000/BTC
        >>> convert_usd_fee_to_base_coins(10.0, "BTC", 50000.0)
        0.0002  # 0.0002 BTC

    Note:
        For stablecoin pairs (USDT, USDC, etc.), the conversion assumes
        1 coin ≈ 1 USD, which is accurate for major stablecoins.

        For non-stablecoin pairs, the conversion divides the USD fee
        by the current price to get the coin amount.
    """
    # Validate base currency is uppercase for consistent comparison
    base_currency_upper = base_currency.upper()

    # For stablecoins: 1 USD ≈ 1 coin
    if base_currency_upper in STABLE_QUOTES:
        logger.debug(
            f"Converting ${fee_usd} fee for stablecoin {base_currency}: "
            f"{fee_usd} {base_currency}"
        )
        return fee_usd

    # For non-stablecoins: divide USD by price to get coins
    if base_price_usd <= 0:
        logger.error(
            f"Invalid price for {base_currency}: {base_price_usd}. "
            f"Cannot convert withdrawal fee. Returning 0."
        )
        return 0.0

    fee_coins = fee_usd / base_price_usd
    logger.debug(
        f"Converting ${fee_usd} fee for {base_currency} at ${base_price_usd}: "
        f"{fee_coins} {base_currency}"
    )

    return fee_coins
