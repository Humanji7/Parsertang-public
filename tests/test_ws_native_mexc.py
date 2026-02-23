from parsertang.ws_native.mexc import parse_mexc_bbo


MEXC_MSG = {
    "channel": "spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT",
    "publicbookticker": {
        "bidprice": "93387.28",
        "bidquantity": "3.73485",
        "askprice": "93387.29",
        "askquantity": "7.669875",
    },
    "symbol": "BTCUSDT",
    "sendtime": 1736412092433,
}


def test_parse_mexc_bbo():
    ev = parse_mexc_bbo(MEXC_MSG, ts_recv=1_700_000_000_000)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 93387.28
    assert ev.ask == 93387.29


def test_mexc_client_parse_binary():
    from parsertang.ws_native.clients import MexcClient
    from parsertang.ws_native.mexc_proto_loader import get_wrapper_message_class

    wrapper_cls = get_wrapper_message_class()
    wrapper = wrapper_cls()
    wrapper.channel = "spot@public.aggre.bookTicker.v3.api.pb@100ms@BTCUSDT"
    wrapper.symbol = "BTCUSDT"
    wrapper.sendTime = 1_700_000_000_000
    wrapper.publicAggreBookTicker.bidPrice = "100.0"
    wrapper.publicAggreBookTicker.askPrice = "101.0"
    data = wrapper.SerializeToString()

    ev = MexcClient().parse(data, ts_recv=1_700_000_000_100)
    assert ev.symbol == "BTC/USDT"
    assert ev.bid == 100.0
    assert ev.ask == 101.0
