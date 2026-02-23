from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class RefreshTrace:
    reason: str
    start_ts: float
    steps: Dict[str, float] = field(default_factory=dict)
    first_update: Optional[Tuple[str, str, float]] = None

    def mark(self, step: str, ts: float) -> None:
        self.steps[step] = ts

    def mark_first_update(self, ex_id: str, symbol: str, ts: float) -> None:
        if self.first_update is None:
            self.first_update = (ex_id, symbol, ts)

    def has_start(self) -> bool:
        return "start" in self.steps

    def _step_status(self, step: str) -> str:
        return "ok" if step in self.steps else "none"

    def _format_first_update(self) -> str:
        if not self.first_update:
            return "none"
        ex_id, symbol, ts = self.first_update
        delta = ts - self.start_ts
        return f"{ex_id}:{symbol}@{delta:.1f}s"

    def summary(self) -> str:
        return (
            "REFRESH TRACE | "
            f"reason={self.reason} "
            f"close={self._step_status('close')} "
            f"init={self._step_status('init')} "
            f"subscribe={self._step_status('subscribe')} "
            f"first_update={self._format_first_update()}"
        )
