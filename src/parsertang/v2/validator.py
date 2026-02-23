from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import math
from typing import TYPE_CHECKING

from parsertang.config import settings
from parsertang.liquidity import liquidity_usd_within_window

if TYPE_CHECKING:
    from parsertang.exchanges import ExchangeGateway


_ORDERBOOK_FETCH_POOL = ThreadPoolExecutor(max_workers=4)


@dataclass
class ValidationResult:
    ok: bool
    reason: str
    rest_buy: float | None = None
    rest_sell: float | None = None


class Validator:
    def __init__(self, gateway: ExchangeGateway | None):
        self.gateway = gateway
        self._tick_cache: dict[tuple[str, str], float] = {}

    def _price_tick(self, ex_id: str, symbol: str) -> float | None:
        key = (ex_id, symbol)
        if key in self._tick_cache:
            return self._tick_cache[key]

        if not self.gateway:
            return None
        exchanges = getattr(self.gateway, "exchanges", None)
        if not exchanges:
            return None
        ex = exchanges.get(ex_id)
        if not ex:
            return None

        market = None
        try:
            market = ex.market(symbol)
        except Exception:
            market = getattr(ex, "markets", {}).get(symbol)

        tick = None
        if isinstance(market, dict):
            precision = (market.get("precision") or {}).get("price")
            if isinstance(precision, (int, float)) and precision >= 0:
                try:
                    tick = 10 ** (-int(precision))
                except Exception:
                    tick = None

            if tick is None:
                info = market.get("info") or {}
                for key_name in (
                    "tickSize",
                    "tick_size",
                    "priceIncrement",
                    "price_increment",
                    "minPrice",
                    "min_price",
                ):
                    raw = info.get(key_name)
                    if raw is None:
                        continue
                    try:
                        candidate = float(raw)
                    except (TypeError, ValueError):
                        continue
                    if candidate > 0:
                        tick = candidate
                        break

        if tick is not None:
            tick = float(tick)
            if not math.isfinite(tick) or tick <= 0:
                tick = None

        if tick is not None:
            self._tick_cache[key] = tick
        return tick

    def validate(
        self,
        *,
        symbol: str,
        buy_ex: str,
        sell_ex: str,
        buy_price: float,
        sell_price: float,
    ) -> ValidationResult:
        if not self.gateway:
            return ValidationResult(ok=False, reason="no_gateway")

        try:
            # Reduce end-to-end validation latency:
            # fetch both exchanges' orderbooks concurrently (2 independent REST calls).
            limit = settings.orderbook_limit
            if buy_ex == sell_ex:
                buy_bids, buy_asks = self.gateway.fetch_order_book(
                    buy_ex, symbol, limit=limit
                )
                sell_bids, sell_asks = buy_bids, buy_asks
            else:
                fut_buy = _ORDERBOOK_FETCH_POOL.submit(
                    self.gateway.fetch_order_book, buy_ex, symbol, limit=limit
                )
                fut_sell = _ORDERBOOK_FETCH_POOL.submit(
                    self.gateway.fetch_order_book, sell_ex, symbol, limit=limit
                )
                buy_bids, buy_asks = fut_buy.result()
                sell_bids, sell_asks = fut_sell.result()
        except Exception:
            return ValidationResult(ok=False, reason="rest_error")

        if not buy_bids or not buy_asks or not sell_bids or not sell_asks:
            return ValidationResult(ok=False, reason="rest_empty")

        rest_buy_ask = buy_asks[0][0]
        rest_sell_bid = sell_bids[0][0]
        tol_pct = settings.v2_validation_price_tolerance_pct
        tick_mult = settings.v2_validation_tick_multiplier

        buy_tick = self._price_tick(buy_ex, symbol)
        sell_tick = self._price_tick(sell_ex, symbol)
        buy_tol_abs = buy_price * (tol_pct / 100.0)
        sell_tol_abs = sell_price * (tol_pct / 100.0)
        if buy_tick:
            buy_tol_abs = max(buy_tol_abs, buy_tick * tick_mult)
        if sell_tick:
            sell_tol_abs = max(sell_tol_abs, sell_tick * tick_mult)

        if rest_buy_ask > buy_price + buy_tol_abs:
            return ValidationResult(
                ok=False,
                reason="rest_buy_price",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )
        if rest_sell_bid < sell_price - sell_tol_abs:
            return ValidationResult(
                ok=False,
                reason="rest_sell_price",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )

        sell_bid_liq, _ = liquidity_usd_within_window(
            sell_bids, sell_asks, settings.liquidity_window_pct
        )
        _, buy_ask_liq = liquidity_usd_within_window(
            buy_bids, buy_asks, settings.liquidity_window_pct
        )

        if sell_bid_liq < settings.liquidity_usd_threshold:
            return ValidationResult(
                ok=False,
                reason="rest_bid_liq",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )
        if buy_ask_liq < settings.liquidity_usd_threshold:
            return ValidationResult(
                ok=False,
                reason="rest_ask_liq",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )

        if sell_bid_liq < settings.trade_volume_usd:
            return ValidationResult(
                ok=False,
                reason="rest_bid_depth",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )
        if buy_ask_liq < settings.trade_volume_usd:
            return ValidationResult(
                ok=False,
                reason="rest_ask_depth",
                rest_buy=rest_buy_ask,
                rest_sell=rest_sell_bid,
            )

        return ValidationResult(
            ok=True,
            reason="ok",
            rest_buy=rest_buy_ask,
            rest_sell=rest_sell_bid,
        )
