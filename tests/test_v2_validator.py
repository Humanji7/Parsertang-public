from parsertang.config import settings
from parsertang.v2.validator import Validator


class FakeGateway:
    def __init__(
        self,
        orderbooks: dict[
            str, tuple[list[tuple[float, float]], list[tuple[float, float]]]
        ],
    ):
        self._orderbooks = orderbooks

    def fetch_order_book(self, ex_id: str, symbol: str, limit: int = 20):
        return self._orderbooks[ex_id]


def test_validator_requires_gateway():
    v = Validator(gateway=None)
    result = v.validate(
        symbol="AAA/USDT",
        buy_ex="bybit",
        sell_ex="okx",
        buy_price=100.0,
        sell_price=101.0,
    )
    assert result.ok is False
    assert result.reason == "no_gateway"


def test_validator_accepts_rest_ok():
    settings.v2_validation_price_tolerance_pct = 0.1
    settings.liquidity_window_pct = 0.1
    settings.liquidity_usd_threshold = 100.0
    settings.trade_volume_usd = 50.0

    buy_asks = [(100.0, 20.0)]
    buy_bids = [(99.9, 5.0)]
    sell_bids = [(101.0, 20.0)]
    sell_asks = [(101.1, 5.0)]

    gw = FakeGateway(
        {
            "bybit": (buy_bids, buy_asks),
            "okx": (sell_bids, sell_asks),
        }
    )

    v = Validator(gateway=gw)
    result = v.validate(
        symbol="AAA/USDT",
        buy_ex="bybit",
        sell_ex="okx",
        buy_price=100.0,
        sell_price=101.0,
    )

    assert result.ok is True
    assert result.reason == "ok"


def test_validator_rejects_price_drift():
    settings.v2_validation_price_tolerance_pct = 0.1
    settings.liquidity_window_pct = 0.1
    settings.liquidity_usd_threshold = 100.0
    settings.trade_volume_usd = 50.0

    buy_asks = [(101.0, 20.0)]
    buy_bids = [(100.9, 5.0)]
    sell_bids = [(101.0, 20.0)]
    sell_asks = [(101.1, 5.0)]

    gw = FakeGateway(
        {
            "bybit": (buy_bids, buy_asks),
            "okx": (sell_bids, sell_asks),
        }
    )

    v = Validator(gateway=gw)
    result = v.validate(
        symbol="AAA/USDT",
        buy_ex="bybit",
        sell_ex="okx",
        buy_price=100.0,
        sell_price=101.0,
    )

    assert result.ok is False
    assert result.reason == "rest_buy_price"


def test_validator_fetches_orderbooks_concurrently():
    import threading

    settings.v2_validation_price_tolerance_pct = 0.1
    settings.liquidity_window_pct = 0.1
    settings.liquidity_usd_threshold = 100.0
    settings.trade_volume_usd = 50.0

    buy_asks = [(100.0, 20.0)]
    buy_bids = [(99.9, 5.0)]
    sell_bids = [(101.0, 20.0)]
    sell_asks = [(101.1, 5.0)]

    class BarrierGateway:
        def __init__(self):
            self._barrier = threading.Barrier(2, timeout=0.5)
            self._orderbooks = {
                "bybit": (buy_bids, buy_asks),
                "okx": (sell_bids, sell_asks),
            }

        def fetch_order_book(self, ex_id: str, symbol: str, limit: int = 20):
            # If validate() fetches sequentially in one thread, the barrier will
            # time out and raise, causing rest_error. With concurrent fetches,
            # both calls reach the barrier and proceed.
            self._barrier.wait()
            return self._orderbooks[ex_id]

    v = Validator(gateway=BarrierGateway())
    result = v.validate(
        symbol="AAA/USDT",
        buy_ex="bybit",
        sell_ex="okx",
        buy_price=100.0,
        sell_price=101.0,
    )

    assert result.ok is True
    assert result.reason == "ok"
