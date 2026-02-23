import time
from dataclasses import dataclass


@dataclass
class DepthSnapshot:
    bids: list
    asks: list
    ts: float


class DepthCache:
    def __init__(self, gateway, refresh_seconds: int, ttl_seconds: int, limit: int = 50):
        self.gateway = gateway
        self.refresh_seconds = refresh_seconds
        self.ttl_seconds = ttl_seconds
        self.limit = int(limit)
        self._cache: dict[tuple[str, str], DepthSnapshot] = {}
        self._last_refresh: dict[tuple[str, str], float] = {}

    def refresh(self, ex_id: str, symbol: str, now: float | None = None) -> None:
        now = now or time.time()
        key = (ex_id, symbol)
        last = self._last_refresh.get(key, 0)
        if now - last < self.refresh_seconds:
            return
        bids, asks = self.gateway.fetch_order_book(ex_id, symbol, limit=self.limit)
        from parsertang.core.orderbook_processor import parse_orderbook_entries

        bids = parse_orderbook_entries(bids)
        asks = parse_orderbook_entries(asks)
        self._cache[key] = DepthSnapshot(bids=bids, asks=asks, ts=now)
        self._last_refresh[key] = now

    def get(self, ex_id: str, symbol: str, now: float | None = None) -> DepthSnapshot | None:
        now = now or time.time()
        snap = self._cache.get((ex_id, symbol))
        if not snap:
            return None
        if now - snap.ts > self.ttl_seconds:
            return None
        return snap
