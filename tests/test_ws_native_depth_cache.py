import time

from parsertang.ws_native.depth_cache import DepthCache


class DummyGateway:
    def __init__(self):
        self.calls = 0
        self.last_limit = None

    def fetch_order_book(self, ex_id, symbol, limit=20):
        self.calls += 1
        self.last_limit = limit
        return [[100.0, 1.0]], [[101.0, 1.0]]


def test_depth_cache_refresh_and_ttl():
    gw = DummyGateway()
    cache = DepthCache(gw, refresh_seconds=1, ttl_seconds=2, limit=50)
    cache.refresh("bybit", "BTC/USDT", now=time.time())
    snap = cache.get("bybit", "BTC/USDT", now=time.time())
    assert snap is not None
    assert gw.calls == 1
    assert gw.last_limit == 50
