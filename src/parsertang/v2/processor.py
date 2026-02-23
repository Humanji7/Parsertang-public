from typing import Dict, Tuple

from .models import Event


class Processor:
    def __init__(self):
        self._state: Dict[Tuple[str, str], Event] = {}

    def handle(self, event: Event) -> None:
        key = (event.ex, event.symbol)
        current = self._state.get(key)
        if current and event.ts_recv < current.ts_recv:
            # ignore out-of-order/older events
            return
        self._state[key] = event

    def snapshot(self) -> Dict[Tuple[str, str], Event]:
        return dict(self._state)

    def snapshot_fresh(
        self, now_ts: float, max_age_ms: float
    ) -> Dict[Tuple[str, str], Event]:
        return {
            key: ev
            for key, ev in self._state.items()
            if now_ts - ev.ts_recv <= max_age_ms
        }

    def lag(self, now_ts: float) -> Dict[Tuple[str, str], float]:
        return {key: now_ts - ev.ts_recv for key, ev in self._state.items()}
