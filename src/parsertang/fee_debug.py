from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


def parse_debug_fee_symbols(raw: str | None) -> set[str] | None:
    """Parse DEBUG_FEE_SYMBOLS.

    Supported:
    - None / empty → disabled
    - "*" → all symbols
    - CSV like "APT/USDT,ARB/USDT"
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped == "*":
        return {"*"}
    return {item.strip().upper() for item in stripped.split(",") if item.strip()}


def is_fee_debug_enabled(symbol: str, enabled: set[str] | None) -> bool:
    if not enabled:
        return False
    if "*" in enabled:
        return True
    return symbol.upper() in enabled


@dataclass
class RateLimiter:
    """In-process rate limiter for logs (asyncio-friendly, no locks)."""

    interval_seconds: float
    clock: Callable[[], float] = time.monotonic

    def __post_init__(self) -> None:
        if self.interval_seconds < 0:
            raise ValueError("interval_seconds must be >= 0")
        self._last_ts_by_key: dict[str, float] = {}

    def should_log(self, key: str) -> bool:
        if self.interval_seconds == 0:
            return True
        now = float(self.clock())
        last = self._last_ts_by_key.get(key)
        if last is not None and (now - last) < self.interval_seconds:
            return False
        self._last_ts_by_key[key] = now
        return True
