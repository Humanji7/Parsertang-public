import logging

from parsertang.exchanges import ExchangeGateway


def test_load_markets_logs_empty_return(caplog):
    class DummyExchange:
        def load_markets(self):
            return {}

    gw = ExchangeGateway.__new__(ExchangeGateway)
    gw.exchanges = {"gate": DummyExchange()}
    gw._monitor = None

    with caplog.at_level(logging.WARNING):
        markets = gw.load_markets()

    assert markets["gate"] == {}
    assert "load_markets returned empty for gate" in caplog.text
