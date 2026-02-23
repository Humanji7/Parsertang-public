from parsertang.ws_native.bybit import parse_bybit_bbo


BYBIT_MSG = {
    "topic": "orderbook.1.BTCUSDT",
    "type": "snapshot",
    "ts": 1672304484978,
    "data": {
        "s": "BTCUSDT",
        "b": [["16493.50", "0.006"]],
        "a": [["16611.00", "0.029"]],
    },
    "cts": 1672304484976,
}


def test_parse_bybit_bbo():
    ev = parse_bybit_bbo(BYBIT_MSG, ts_recv=1_700_000_000_000)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 16493.50
    assert ev.ask == 16611.00


def test_parse_bybit_bbo_list_payload():
    msg = {
        "topic": "orderbook.1.BTCUSDT",
        "ts": 1672304484978,
        "data": [
            {
                "s": "BTCUSDT",
                "b": [["30000", "1"]],
                "a": [["30001", "2"]],
            }
        ],
    }
    ev = parse_bybit_bbo(msg, ts_recv=1_700_000_000_000)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 30000.0
    assert ev.ask == 30001.0
