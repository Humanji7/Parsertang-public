"""Fee statistics tracking for Daily Fee Report.

Tracks currencies that are rejected due to missing fallback data.
Sends daily report to technical Telegram channel.

Thread-safe: uses asyncio.Lock for concurrent access.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Coroutine

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Report settings
MAX_REPORT_ITEMS = 50
REPORT_INTERVAL_SECONDS = 86400  # 24 hours
RETRY_INTERVAL_SECONDS = 3600  # 1 hour on error


class FeeStatistics:
    """Thread-safe tracking of missing fallback currencies.

    Usage:
        fee_stats = FeeStatistics()

        # Record missing fallback (from fee_calculator)
        await fee_stats.record_missing_fallback("okx", "USD1")

        # Get and clear stats (for daily report)
        stats = await fee_stats.get_and_clear_stats()
    """

    def __init__(self) -> None:
        self._missing_fallbacks: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._lock = asyncio.Lock()

    async def record_missing_fallback(self, exchange_id: str, currency: str) -> None:
        """Record a currency that needs fallback data.

        Thread-safe: acquires lock before mutation.

        Args:
            exchange_id: Exchange ID (e.g., "okx")
            currency: Currency code (e.g., "USD1")
        """
        async with self._lock:
            self._missing_fallbacks[(exchange_id, currency)] += 1

    async def get_and_clear_stats(self) -> dict[tuple[str, str], int]:
        """Get current stats and clear internal state.

        Thread-safe: acquires lock, copies data, clears internal dict.

        Returns:
            Dict mapping (exchange_id, currency) → rejection count
        """
        async with self._lock:
            stats = dict(self._missing_fallbacks)
            self._missing_fallbacks.clear()
            return stats


def format_daily_fee_report(stats: dict[tuple[str, str], int]) -> str | None:
    """Format daily fee report message.

    Args:
        stats: Dict mapping (exchange_id, currency) → rejection count

    Returns:
        Formatted message string, or None if no problems
    """
    if not stats:
        return None

    # Sort by count descending
    sorted_items = sorted(stats.items(), key=lambda x: -x[1])

    lines = ["🔴 Daily Fee Report: Missing Fallback", ""]

    # Show top MAX_REPORT_ITEMS
    shown = sorted_items[:MAX_REPORT_ITEMS]
    for (exchange_id, currency), count in shown:
        lines.append(f"• {exchange_id}/{currency}: {count} отказов")

    # Show overflow count
    overflow = len(sorted_items) - MAX_REPORT_ITEMS
    if overflow > 0:
        lines.append(f"... и ещё {overflow} валют")

    lines.append("")
    lines.append(f"Всего проблемных валют: {len(stats)}")

    return "\n".join(lines)


async def daily_fee_report_task(
    fee_stats: FeeStatistics,
    send_fn: Callable[[str], Coroutine],
    interval_seconds: int = REPORT_INTERVAL_SECONDS,
) -> None:
    """Background task that sends daily fee report.

    Runs forever, sending report every interval_seconds.
    On error, retries after RETRY_INTERVAL_SECONDS.

    Args:
        fee_stats: FeeStatistics instance to get stats from
        send_fn: Async function to send message (e.g., alerts.send_tech)
        interval_seconds: Seconds between reports (default: 24h)
    """
    logger.info("Daily fee report task started (interval=%ds)", interval_seconds)

    while True:
        try:
            await asyncio.sleep(interval_seconds)

            # Get and clear stats
            stats = await fee_stats.get_and_clear_stats()

            if not stats:
                logger.debug("Daily fee report: no missing fallbacks")
                continue

            # Format and send report
            report = format_daily_fee_report(stats)
            if report:
                await send_fn(report)
                logger.info(
                    "Daily fee report sent: %d currencies with issues",
                    len(stats),
                )

        except asyncio.CancelledError:
            logger.info("Daily fee report task cancelled")
            raise

        except Exception as e:
            logger.error(
                "Daily fee report task error: %s, retrying in %ds",
                e,
                RETRY_INTERVAL_SECONDS,
            )
            await asyncio.sleep(RETRY_INTERVAL_SECONDS)
