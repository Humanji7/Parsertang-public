"""Static fallback withdrawal fees for currencies missing from exchange APIs.

SPEC-FALLBACK-001: Provides hardcoded fees when dynamic API data is unavailable
but exchange API is responding (specific currency missing, not API down).
"""

from dataclasses import dataclass, field
from datetime import date


@dataclass
class StaticFee:
    """Static withdrawal fee data from external sources."""

    fee: float  # Fee in base currency
    source: str  # Data source (e.g., "okx_website")
    verified_at: str  # ISO date when verified (e.g., "2025-01-03")


@dataclass
class FallbackStats:
    """Statistics for fallback fee usage and rejections."""

    # Key: (exchange, currency, network), Value: count
    rejection_counts: dict[tuple[str, str, str], int] = field(default_factory=dict)
    fallback_usage: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def record_rejection(self, exchange: str, currency: str, network: str) -> None:
        """Record a rejection (opportunity rejected due to low confidence)."""
        key = (exchange, currency, network)
        self.rejection_counts[key] = self.rejection_counts.get(key, 0) + 1

    def record_fallback_usage(self, exchange: str, currency: str, network: str) -> None:
        """Record fallback fee usage."""
        key = (exchange, currency, network)
        self.fallback_usage[key] = self.fallback_usage.get(key, 0) + 1

    def reset_daily_stats(self) -> None:
        """Reset all counters for new day."""
        self.rejection_counts.clear()
        self.fallback_usage.clear()


# Key: (exchange, currency, network) - all lowercase exchange, uppercase currency/network
# Data source: withdrawalfees.com (2025-01-03)
FALLBACK_FEES: dict[tuple[str, str, str], StaticFee] = {
    # OKX USDC withdrawal fees
    ("okx", "USDC", "APT"): StaticFee(0.0002, "okx_website", "2025-01-03"),
    ("okx", "USDC", "BASE"): StaticFee(0.0003, "okx_website", "2025-01-03"),
    ("okx", "USDC", "AVAX"): StaticFee(0.001, "okx_website", "2025-01-03"),
    ("okx", "USDC", "POLYGON"): StaticFee(0.001, "okx_website", "2025-01-03"),
    ("okx", "USDC", "SUI"): StaticFee(0.001, "okx_website", "2025-01-03"),
    ("okx", "USDC", "ARB"): StaticFee(0.004, "okx_website", "2025-01-03"),
    ("okx", "USDC", "OP"): StaticFee(0.004, "okx_website", "2025-01-03"),
    ("okx", "USDC", "XLAYER"): StaticFee(0.10, "okx_website", "2025-01-03"),
    ("okx", "USDC", "ETH"): StaticFee(0.37, "okx_website", "2025-01-03"),
    ("okx", "USDC", "SOL"): StaticFee(0.37, "okx_website", "2025-01-03"),
    # OKX USD1 withdrawal fees (only ERC20 supported, min 1.5 USD1)
    ("okx", "USD1", "ETH"): StaticFee(0.41, "okx_website", "2025-01-04"),
    ("okx", "USD1", "ERC20"): StaticFee(0.41, "okx_website", "2025-01-04"),
}


def get_fallback_fee(exchange: str, currency: str, network: str) -> StaticFee | None:
    """Get static fallback fee if available."""
    return FALLBACK_FEES.get((exchange.lower(), currency.upper(), network.upper()))


def get_fallback_networks(exchange: str, currency: str) -> set[str]:
    """Get all networks available in fallback for exchange+currency.

    Returns:
        Set of network names (uppercase) or empty set if no fallback data.
    """
    exchange_lower = exchange.lower()
    currency_upper = currency.upper()
    return {
        network
        for (ex, curr, network) in FALLBACK_FEES.keys()
        if ex == exchange_lower and curr == currency_upper
    }


def has_fallback_data(exchange: str, currency: str) -> bool:
    """Check if any fallback data exists for exchange+currency."""
    return len(get_fallback_networks(exchange, currency)) > 0


def calculate_fee_age_days(verified_at: str) -> int:
    """Calculate age of fee data in days since verification date."""
    return (date.today() - date.fromisoformat(verified_at)).days


def get_fee_age_warning(age_days: int) -> str:
    """Get warning string based on fee data age.

    Returns:
        Empty string for fresh data (<=3 days)
        Yellow warning for medium age (4-7 days)
        Red warning for stale data (>7 days)
    """
    if age_days <= 3:
        return ""
    if age_days <= 7:
        return f"⚠️ Fee data is {age_days} days old"
    return f"🔴 Fee data is {age_days} days old (stale)"


def check_fee_drift(
    cache_fees: dict[str, dict[str, dict[str, float]]],
) -> list[dict[str, object]]:
    """Compare fallback fees against dynamic cache fees.

    Args:
        cache_fees: Nested dict {exchange: {currency: {network: fee}}}

    Returns:
        List of drift records for fees differing by >10%
    """
    drifts: list[dict[str, object]] = []

    for (exchange, currency, network), static_fee in FALLBACK_FEES.items():
        # Check if dynamic fee exists in cache
        exchange_cache = cache_fees.get(exchange, {})
        currency_cache = exchange_cache.get(currency, {})
        dynamic_fee = currency_cache.get(network)

        if dynamic_fee is None:
            continue

        # Calculate drift percentage
        drift_pct = abs(dynamic_fee - static_fee.fee) / static_fee.fee * 100

        if drift_pct > 10:
            drifts.append(
                {
                    "exchange": exchange,
                    "currency": currency,
                    "network": network,
                    "fallback_fee": static_fee.fee,
                    "dynamic_fee": dynamic_fee,
                    "drift_pct": drift_pct,
                }
            )

    return drifts


def format_daily_fee_report(stats: FallbackStats) -> str:
    """Format daily fee statistics report.

    Args:
        stats: FallbackStats with rejection_counts and fallback_usage

    Returns:
        Formatted text report for Telegram/logging
    """
    lines = ["📊 Daily Fee Report"]

    # Rejections section
    if stats.rejection_counts:
        lines.append("\n🔴 Rejections (low confidence fees):")
        for (exchange, currency, network), count in sorted(
            stats.rejection_counts.items(), key=lambda x: -x[1]
        ):
            lines.append(f"  • {exchange} {currency}/{network}: {count}x")
    else:
        lines.append("\n✅ No rejections today")

    # Fallback usage section
    if stats.fallback_usage:
        lines.append("\n⚠️ Fallback usage:")
        for (exchange, currency, network), count in sorted(
            stats.fallback_usage.items(), key=lambda x: -x[1]
        ):
            lines.append(f"  • {exchange} {currency}/{network}: {count}x")
    else:
        lines.append("\n✅ No fallback usage today")

    return "\n".join(lines)
