from collections import deque
from typing import Dict, List, Tuple

from .models import Event


class BoundedEventQueue:
    def __init__(self, per_exchange_capacity: int = 100):
        self.per_exchange_capacity = per_exchange_capacity
        self._storage: Dict[str, deque[Tuple[int, Event]]] = {}
        self._counter = 0
        self._drops = 0

    def push(self, event: Event) -> None:
        dq = self._storage.setdefault(event.ex, deque())
        if len(dq) >= self.per_exchange_capacity:
            dq.popleft()
            self._drops += 1
        dq.append((self._counter, event))
        self._counter += 1

    def drain(self) -> List[Event]:
        items: List[Tuple[int, Event]] = []
        for dq in self._storage.values():
            items.extend(dq)
            dq.clear()
        # sort by push order to preserve arrival sequence across exchanges
        items.sort(key=lambda x: x[0])
        return [ev for _, ev in items]

    def __len__(self) -> int:
        return sum(len(dq) for dq in self._storage.values())

    def stats(self) -> Dict[str, int]:
        return {"drops": self._drops, "size": len(self)}
