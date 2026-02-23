from parsertang.ws_native.events import BBOEvent


def test_bbo_event_fields():
    ev = BBOEvent(
        ex="bybit",
        symbol="BTC/USDT",
        bid=100.0,
        ask=101.0,
        ts_ex=1_700_000_000_000,
        ts_recv=1_700_000_000_100,
    )
    assert ev.ex == "bybit"
    assert ev.bid < ev.ask
