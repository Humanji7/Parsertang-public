from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class HealthSnapshot:
    now_ts: float
    multi_ex_symbols: int
    stale_exchanges: int


@dataclass(frozen=True)
class AdaptiveDecision:
    action: str
    reason: str
    limits: dict[str, int]


class AdaptiveSymbolLimiter:
    def __init__(
        self,
        *,
        base_limits: dict[str, int],
        max_limits: dict[str, int],
        core_exchanges: Iterable[str],
        periphery_exchanges: Iterable[str],
        step_core: int,
        step_periphery: int,
        window_seconds: int,
        min_multi_ex_symbols: int,
        max_stale_exchanges: int,
    ) -> None:
        self.base_limits = dict(base_limits)
        self.max_limits = dict(max_limits)
        self.core_exchanges = set(core_exchanges)
        self.periphery_exchanges = set(periphery_exchanges)
        self.step_core = max(0, int(step_core))
        self.step_periphery = max(0, int(step_periphery))
        self.window_seconds = max(1, int(window_seconds))
        self.min_multi_ex_symbols = max(0, int(min_multi_ex_symbols))
        self.max_stale_exchanges = max(0, int(max_stale_exchanges))

        self.current_limits = dict(base_limits)
        self.healthy_since_ts: float | None = None
        self.last_change_ts: float | None = None

    def _is_healthy(self, snapshot: HealthSnapshot) -> bool:
        return (
            snapshot.multi_ex_symbols >= self.min_multi_ex_symbols
            and snapshot.stale_exchanges <= self.max_stale_exchanges
        )

    def evaluate(self, snapshot: HealthSnapshot) -> AdaptiveDecision | None:
        if not self._is_healthy(snapshot):
            self.healthy_since_ts = None
            if self.current_limits != self.base_limits:
                self.current_limits = dict(self.base_limits)
                self.last_change_ts = snapshot.now_ts
                return AdaptiveDecision(
                    action="decrease",
                    reason="unhealthy",
                    limits=dict(self.current_limits),
                )
            return None

        if self.healthy_since_ts is None:
            self.healthy_since_ts = snapshot.now_ts
            return None

        healthy_duration = snapshot.now_ts - self.healthy_since_ts
        if healthy_duration < self.window_seconds:
            return None

        if (
            self.last_change_ts is not None
            and snapshot.now_ts - self.last_change_ts < self.window_seconds
        ):
            return None

        new_limits = dict(self.current_limits)
        for ex_id in self.core_exchanges:
            new_limits[ex_id] = self._apply_step(
                ex_id, new_limits.get(ex_id, 0), self.step_core
            )
        for ex_id in self.periphery_exchanges:
            new_limits[ex_id] = self._apply_step(
                ex_id, new_limits.get(ex_id, 0), self.step_periphery
            )

        if new_limits == self.current_limits:
            return None

        self.current_limits = new_limits
        self.last_change_ts = snapshot.now_ts
        return AdaptiveDecision(
            action="increase",
            reason="healthy_window",
            limits=dict(self.current_limits),
        )

    def _apply_step(self, ex_id: str, current: int, step: int) -> int:
        if step <= 0:
            return current
        max_limit = self.max_limits.get(ex_id, current)
        return min(current + step, max_limit)
