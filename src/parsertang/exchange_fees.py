"""
Static exchange fee database.

Partner-verified fees as of 2025-10-18.
These override any API-fetched fees to ensure accuracy.
"""

from typing import Dict, Tuple


# Exchange fees: (maker_fee_pct, taker_fee_pct)
# Source: Partner verification 2025-10-18
EXCHANGE_FEES: Dict[str, Tuple[float, float]] = {
    # Exchange: (maker%, taker%)
    "bybit": (0.10, 0.10),
    "okx": (0.08, 0.10),
    "kucoin": (0.10, 0.10),
    "htx": (0.20, 0.20),
    "gate": (0.10, 0.10),
    "gateio": (0.10, 0.10),  # Alias
    "mexc": (0.00, 0.05),  # MEXC: 0% maker, 0.05% taker (CRITICAL!)
}


def get_taker_fee(exchange: str) -> float:
    """Get taker fee for exchange (default 0.10% if unknown)."""
    exchange_lower = exchange.lower()
    return EXCHANGE_FEES.get(exchange_lower, (0.10, 0.10))[1]


def get_maker_fee(exchange: str) -> float:
    """Get maker fee for exchange (default 0.10% if unknown)."""
    exchange_lower = exchange.lower()
    return EXCHANGE_FEES.get(exchange_lower, (0.10, 0.10))[0]


def get_fees(exchange: str) -> Tuple[float, float]:
    """Get (maker, taker) fees for exchange."""
    exchange_lower = exchange.lower()
    return EXCHANGE_FEES.get(exchange_lower, (0.10, 0.10))
