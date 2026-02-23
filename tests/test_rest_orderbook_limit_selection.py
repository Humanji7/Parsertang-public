from parsertang.exchanges import select_orderbook_limit


def test_select_orderbook_limit_okx_passthrough() -> None:
    # OKX is WS-limited (books5) but REST can fetch deeper books.
    assert select_orderbook_limit("okx", 200) == 200

