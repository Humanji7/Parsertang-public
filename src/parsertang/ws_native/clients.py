from __future__ import annotations

import asyncio

from parsertang.ws_native.bybit import parse_bybit_bbo
from parsertang.ws_native.client_base import NativeWsClient
from parsertang.ws_native.events import BBOEvent
from parsertang.ws_native.mexc import _norm_symbol, parse_mexc_bbo
from parsertang.ws_native.okx import parse_okx_bbo


class OkxClient(NativeWsClient):
    url = "wss://ws.okx.com:8443/ws/v5/public"

    def build_subscribe(self, symbols: list[str]) -> dict:
        # Use "tickers" instead of "bbo-tbt" for broader availability/stability.
        args = [{"channel": "tickers", "instId": s.replace("/", "-")} for s in symbols]
        if len(args) <= 10:
            return {"op": "subscribe", "args": args}
        payloads = []
        for i in range(0, len(args), 10):
            payloads.append({"op": "subscribe", "args": args[i : i + 10]})
        return payloads

    def parse(self, msg: dict, ts_recv: int):
        return parse_okx_bbo(msg, ts_recv)


class BybitClient(NativeWsClient):
    url = "wss://stream.bybit.com/v5/public/spot"

    def build_subscribe(self, symbols: list[str]) -> dict:
        topics = ["orderbook.1." + s.replace("/", "") for s in symbols]
        if len(topics) <= 10:
            return {"op": "subscribe", "args": topics}
        payloads = []
        for i in range(0, len(topics), 10):
            payloads.append({"op": "subscribe", "args": topics[i : i + 10]})
        return payloads

    def parse(self, msg: dict, ts_recv: int):
        return parse_bybit_bbo(msg, ts_recv)


class MexcClient(NativeWsClient):
    url = "wss://wbs-api.mexc.com/ws"

    _max_symbols_per_conn = 30

    def build_subscribe(self, symbols: list[str]) -> dict:
        params = [
            f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{symbol.replace('/', '')}"
            for symbol in symbols
        ]
        max_params = self._max_symbols_per_conn
        if len(params) <= max_params:
            return {"method": "SUBSCRIPTION", "params": params}
        payloads = []
        for i in range(0, len(params), max_params):
            payloads.append({"method": "SUBSCRIPTION", "params": params[i : i + max_params]})
        return payloads

    async def connect(self, symbols: list[str], on_event):
        # MEXC appears to cap active subscriptions per connection; split into multiple
        # websocket connections when needed.
        max_symbols = int(self._max_symbols_per_conn)
        if len(symbols) <= max_symbols:
            return await NativeWsClient.connect(self, symbols, on_event)

        tasks: list[asyncio.Task] = []
        for i in range(0, len(symbols), max_symbols):
            chunk = symbols[i : i + max_symbols]
            if not chunk:
                continue
            client = MexcClient()
            tasks.append(
                asyncio.create_task(
                    NativeWsClient.connect(client, chunk, on_event),
                    name=f"ws_native_mexc_{(i // max_symbols) + 1}",
                )
            )
        await asyncio.gather(*tasks)

    def parse(self, msg: dict, ts_recv: int):
        if isinstance(msg, (bytes, bytearray)):
            from parsertang.ws_native.mexc_proto_loader import get_wrapper_message_class

            wrapper_cls = get_wrapper_message_class()
            wrapper = wrapper_cls()
            wrapper.ParseFromString(msg)
            if not wrapper.HasField("publicAggreBookTicker"):
                return None
            ticker = wrapper.publicAggreBookTicker
            if not wrapper.symbol:
                return None
            bid = float(ticker.bidPrice)
            ask = float(ticker.askPrice)
            ts_ex = int(wrapper.sendTime or wrapper.createTime or 0)
            return BBOEvent(
                "mexc", _norm_symbol(wrapper.symbol), bid, ask, ts_ex, ts_recv
            )
        if isinstance(msg, dict):
            return parse_mexc_bbo(msg, ts_recv)
        return None
