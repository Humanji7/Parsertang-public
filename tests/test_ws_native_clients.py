import pytest

from parsertang.ws_native.client_base import NativeWsClient


class DummyClient(NativeWsClient):
    url = "wss://example"

    def build_subscribe(self, symbols):
        return {"op": "subscribe", "args": symbols}

    def parse(self, msg, ts_recv):
        return None


def test_build_subscribe_payload():
    client = DummyClient()
    payload = client.build_subscribe(["BTC/USDT"])
    assert payload["op"] == "subscribe"


def test_okx_client_subscribe_payload():
    from parsertang.ws_native.clients import OkxClient

    payload = OkxClient().build_subscribe(["BTC/USDT"])  # no network
    assert payload["op"] == "subscribe"
    assert payload["args"][0]["channel"] == "tickers"
    assert payload["args"][0]["instId"] == "BTC-USDT"


def test_okx_client_subscribe_payload_chunking():
    from parsertang.ws_native.clients import OkxClient

    symbols = [f"SYM{i}/USDT" for i in range(11)]
    payloads = OkxClient().build_subscribe(symbols)
    assert isinstance(payloads, list)
    assert len(payloads) == 2
    assert payloads[0]["op"] == "subscribe"
    assert len(payloads[0]["args"]) == 10


def test_bybit_client_subscribe_payload():
    from parsertang.ws_native.clients import BybitClient

    payload = BybitClient().build_subscribe(["BTC/USDT"])
    assert payload["op"] == "subscribe"
    assert payload["args"][0] == "orderbook.1.BTCUSDT"


def test_bybit_client_subscribe_payload_chunking():
    from parsertang.ws_native.clients import BybitClient

    symbols = [f"SYM{i}/USDT" for i in range(11)]
    payloads = BybitClient().build_subscribe(symbols)
    assert isinstance(payloads, list)
    assert len(payloads) == 2
    assert payloads[0]["op"] == "subscribe"
    assert len(payloads[0]["args"]) == 10


def test_mexc_client_subscribe_payload():
    from parsertang.ws_native.clients import MexcClient

    payload = MexcClient().build_subscribe(["BTC/USDT", "ETH/USDT"])
    assert payload["method"] == "SUBSCRIPTION"
    assert payload["params"][0].endswith("@BTCUSDT")
    assert payload["params"][1].endswith("@ETHUSDT")


def test_mexc_client_subscribe_payload_chunking():
    from parsertang.ws_native.clients import MexcClient

    symbols = [f"SYM{i}/USDT" for i in range(61)]
    payloads = MexcClient().build_subscribe(symbols)
    assert isinstance(payloads, list)
    assert len(payloads) == 3

    flattened: list[str] = []
    for payload in payloads:
        assert payload["method"] == "SUBSCRIPTION"
        assert len(payload["params"]) <= 30
        flattened.extend(payload["params"])

    expected = {
        f"spot@public.aggre.bookTicker.v3.api.pb@100ms@SYM{i}USDT"
        for i in range(61)
    }
    assert set(flattened) == expected


@pytest.mark.asyncio
async def test_mexc_client_connect_chunking(monkeypatch):
    from parsertang.ws_native.client_base import NativeWsClient
    from parsertang.ws_native.clients import MexcClient

    calls: list[list[str]] = []

    async def fake_connect(self, symbols, on_event):  # noqa: ANN001
        calls.append(list(symbols))
        return None

    monkeypatch.setattr(NativeWsClient, "connect", fake_connect, raising=True)

    symbols = [f"SYM{i}/USDT" for i in range(61)]
    await MexcClient().connect(symbols, lambda ev: None)

    assert len(calls) == 3
    assert all(0 < len(chunk) <= 30 for chunk in calls)
    flattened = [sym for chunk in calls for sym in chunk]
    assert set(flattened) == set(symbols)
