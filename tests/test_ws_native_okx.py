from parsertang.ws_native.okx import parse_okx_bbo


OKX_MSG = {
    "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
    "data": [
        {
            "asks": [["8446", "95", "0", "3"]],
            "bids": [["8445", "12", "0", "1"]],
            "ts": "1597026383085",
        }
    ],
}

OKX_TICKERS_MSG = {
    "arg": {"channel": "tickers", "instId": "BTC-USDT"},
    "data": [
        {
            "instId": "BTC-USDT",
            "bidPx": "8445",
            "askPx": "8446",
            "ts": "1597026383085",
        }
    ],
}


def test_parse_okx_bbo():
    ev = parse_okx_bbo(OKX_MSG, ts_recv=1_700_000_000_000)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 8445.0
    assert ev.ask == 8446.0


def test_parse_okx_tickers():
    ev = parse_okx_bbo(OKX_TICKERS_MSG, ts_recv=1_700_000_000_000)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 8445.0
    assert ev.ask == 8446.0
