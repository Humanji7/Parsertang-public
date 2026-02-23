from __future__ import annotations

import asyncio
import inspect
import logging
import re
from typing import Any, Callable, Dict, List

from parsertang.config import (
    BATCH_EXCLUDED_EXCHANGES,
    SUPPORTED_ORDERBOOK_LIMITS,
    WS_ID_ALIASES,
    settings,
)
from parsertang.utils.exchange_credentials import build_exchange_config

ccxtpro = None

try:
    import ccxt.pro as ccxtpro  # type: ignore[attr-defined]
except Exception:  # noqa: BLE001
    try:
        import ccxtpro  # type: ignore
    except Exception:  # noqa: BLE001
        ccxtpro = None

logger = logging.getLogger(__name__)


# Default timeout for ccxt.pro watch_* awaits.
# Without an explicit timeout, some exchanges can get stuck awaiting forever (no updates, no exceptions).
WS_WATCH_TIMEOUT_SECONDS = 30.0
WS_MARKETS_LOAD_TIMEOUT_SECONDS = 60.0

_BYBIT_TIME_SKEW_RE = re.compile(
    r"req_timestamp\[(\d+)\].*?server_timestamp\[(\d+)\].*?recv_window\[(\d+)\]"
)


def _is_non_retriable_ws_error_message(message: str) -> bool:
    """Return True if the WS error is permanent for a given symbol.

    Some exchanges reply with "invalid symbol" when a market is delisted or
    temporarily unavailable. Retrying forever only creates log spam and can
    destabilize WS keepalive handling.
    """
    msg = message.lower()
    return (
        "invalid symbol" in msg
        or "symbol not found" in msg
        or ("bad-request" in msg and "symbol" in msg)
    )


def _is_ccxt_client_keyerror(e: Exception) -> bool:
    """Check if exception is the known ccxt KeyError bug in client cleanup.

    ccxt has a race condition where `del self.clients[client.url]` is called
    without checking if the key exists (lines 542, 551, 557, 579 in
    async_support/base/exchange.py). This happens on timeout/disconnect.

    See: ccxt/ccxt#20992, ccxt/ccxt#16499 for related issues.
    """
    if not isinstance(e, KeyError):
        return False
    # Check if it's a WS URL being deleted
    key = str(e.args[0]) if e.args else ""
    return key.startswith("wss://") or key.startswith("ws://")


def _extract_bybit_time_skew(message: str) -> dict[str, int] | None:
    match = _BYBIT_TIME_SKEW_RE.search(message)
    if not match:
        return None
    req_ts = int(match.group(1))
    server_ts = int(match.group(2))
    recv_window = int(match.group(3))
    return {
        "req_ts": req_ts,
        "server_ts": server_ts,
        "recv_window": recv_window,
        "diff_ms": server_ts - req_ts,
    }


class Streams:
    def __init__(
        self,
        exchange_ids: List[str],
        proxy_config: Dict | None = None,
        *,
        preloaded_markets_by_exchange: dict[str, dict] | None = None,
    ):
        if not ccxtpro:
            raise RuntimeError("ccxt.pro not available; install to use WS streams")
        self._preloaded_markets_by_exchange = preloaded_markets_by_exchange or {}
        self.exchanges: Dict[str, Any] = {}
        self.orderbook_limits: Dict[str, int] = {}
        self.init_status: dict[str, dict[str, str]] = {}
        for ex_id in exchange_ids:
            ex = self.create_exchange(ex_id, proxy_config=proxy_config)
            if ex is not None:
                self.exchanges[ex_id] = ex

    def create_exchange(
        self, ex_id: str, proxy_config: Dict | None = None
    ) -> Any | None:
        class_name = WS_ID_ALIASES.get(ex_id, ex_id)
        if not hasattr(ccxtpro, class_name):
            logger.warning("WS UNSUPPORTED %s -> %s", ex_id, class_name)
            self.init_status[ex_id] = {"status": "unsupported"}
            return None
        try:
            cls = getattr(ccxtpro, class_name)

            # Build config with API credentials (needed for OKX batch subscriptions)
            config = build_exchange_config(ex_id, settings, proxy_config)
            config["enableRateLimit"] = True
            # Gate.io: ccxt may call fetchCurrencies() inside load_markets(), which can fail
            # and block WS workers from starting. We don't need currencies for WS orderbooks.
            if ex_id == "gate":
                opts = config.get("options")
                if not isinstance(opts, dict):
                    opts = {}
                    config["options"] = opts
                opts["fetchCurrencies"] = False
                # Gate load_markets() can be slow/heavy; restrict to spot markets only.
                opts.setdefault("defaultType", "spot")
                fetch_markets = opts.get("fetchMarkets")
                if not isinstance(fetch_markets, dict):
                    fetch_markets = {}
                    opts["fetchMarkets"] = fetch_markets
                fetch_markets["types"] = ["spot"]
                # Keep WS startup bounded (ms).
                config.setdefault("timeout", 10_000)

            ex = cls(config)
            self.init_status[ex_id] = {"status": "ok"}
            if ex_id == "gate" and isinstance(getattr(ex, "has", None), dict):
                ex.has["fetchCurrencies"] = False
            preloaded = self._preloaded_markets_by_exchange.get(ex_id)
            if isinstance(preloaded, dict) and preloaded:
                setter = getattr(ex, "set_markets", None)
                if callable(setter):
                    try:
                        setter(preloaded)
                    except Exception:  # noqa: BLE001
                        # Fall back to load_markets() later if set_markets() fails.
                        pass

            # HTX: Patch URLs directly since ccxt may ignore hostname param
            # for some endpoints. Singapore geo-block bypass.
            if ex_id == "htx" and ex.urls:
                patched = 0
                api_urls = ex.urls.get("api")
                if isinstance(api_urls, dict):
                    for key in list(api_urls.keys()):
                        url = api_urls.get(key)
                        if isinstance(url, str) and "{hostname}" in url:
                            api_urls[key] = url.replace(
                                "{hostname}", "api-aws.huobi.pro"
                            )
                            patched += 1
                    if patched:
                        logger.info(
                            "HTX WS | Patched %d API URLs to api-aws.huobi.pro",
                            patched,
                        )
            return ex
        except Exception as e:
            logger.error("Failed to init WS exchange %s (%s): %s", ex_id, class_name, e)
            self.init_status[ex_id] = {"status": "failed", "error": str(e)}
            return None

    async def _maybe_await(self, value: Any) -> Any:
        return await value if inspect.isawaitable(value) else value

    async def _ensure_markets_loaded(self, ex_id: str, ex: Any) -> bool:
        """Preload WS exchange markets once to avoid per-symbol init storms."""
        try:
            opener = getattr(ex, "open", None)
            if callable(opener):
                opener()

            existing = getattr(ex, "markets", None)
            if isinstance(existing, dict) and existing:
                return True
            loader = getattr(ex, "load_markets", None)
            if not loader:
                return True

            async def _load() -> Any:
                return await self._maybe_await(loader())

            markets = await asyncio.wait_for(
                _load(),
                timeout=WS_MARKETS_LOAD_TIMEOUT_SECONDS,
            )
            if isinstance(markets, dict) and markets:
                logger.info("WS MARKETS | %s loaded markets=%d", ex_id, len(markets))
            else:
                logger.info("WS MARKETS | %s loaded markets", ex_id)
            return True
        except Exception as e:  # noqa: BLE001
            err = str(e) or repr(e)
            logger.error("WS MARKETS FAILED | %s: %s", ex_id, err[:200])
            if ex_id == "bybit":
                skew = _extract_bybit_time_skew(err)
                if skew:
                    logger.warning(
                        "WS MARKETS TIME SKEW | %s req_ts=%d server_ts=%d diff_ms=%d recv_window=%d",
                        ex_id,
                        skew["req_ts"],
                        skew["server_ts"],
                        skew["diff_ms"],
                        skew["recv_window"],
                    )
            return False

    def _log_retry(
        self, prefix: str, context: str, error: str, attempt: int, delay: float
    ) -> None:
        msg = f"{prefix} {context}: {error} (attempt {attempt}, retry in {delay:.1f}s)"
        if attempt <= 3:
            logger.info(msg)
        elif attempt <= 7:
            logger.warning(msg)
        else:
            logger.error(msg)

    async def _batch_worker(
        self,
        ex_id: str,
        ex: Any,
        symbols: List[str],
        on_update: Callable[[str, str, Dict], None],
        reconnect_attempts: Dict[str | tuple[str, str], int],
    ) -> None:
        reconnect_attempts[ex_id] = 0
        limit = select_ob_limit(ex_id, settings.orderbook_limit)[0]
        connected = False
        # Memory is managed globally by clear_exchange_caches() in main.py

        if not symbols:
            logger.info(
                "WS SKIP | %s no symbols allocated; not starting batch worker",
                ex_id,
            )
            return

        logger.info(
            "WS BATCH | %s subscribing to %d symbols (limit=%d)",
            ex_id,
            len(symbols),
            limit,
        )

        while True:
            try:
                ob = await asyncio.wait_for(
                    ex.watch_order_book_for_symbols(symbols, limit=limit),
                    timeout=WS_WATCH_TIMEOUT_SECONDS,
                )
                symbol = ob.get("symbol", "unknown")

                if not connected:
                    logger.info("WS BATCH | %s connected successfully", ex_id)
                    connected = True
                elif reconnect_attempts.get(ex_id, 0) > 0:
                    logger.info(
                        "WS BATCH RECONNECT | %s recovered after %d attempts",
                        ex_id,
                        reconnect_attempts[ex_id],
                    )
                reconnect_attempts[ex_id] = 0

                on_update(ex_id, symbol, ob)

            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    logger.warning(
                        "WS BATCH TIMEOUT | %s timeout=%.3fs symbols=%d limit=%d",
                        ex_id,
                        WS_WATCH_TIMEOUT_SECONDS,
                        len(symbols),
                        limit,
                    )
                connected = False
                attempt = reconnect_attempts.get(ex_id, 0) + 1

                if _is_ccxt_client_keyerror(e):
                    logger.warning(
                        "WS BATCH CCXT BUG | %s KeyError in client cleanup, reconnecting...",
                        ex_id,
                    )
                    delay = 2.0
                else:
                    delay = min(1.5 * (2 ** (attempt - 1)), 60.0)

                reconnect_attempts[ex_id] = attempt
                self._log_retry("WS BATCH", ex_id, str(e)[:100], attempt, delay)
                await asyncio.sleep(delay)

    async def _symbol_worker(
        self,
        ex_id: str,
        ex: Any,
        symbol: str,
        on_update: Callable[[str, str, Dict], None],
        reconnect_attempts: Dict[str | tuple[str, str], int],
    ) -> None:
        symbol_key = (ex_id, symbol)
        reconnect_attempts[symbol_key] = 0
        # Memory is managed globally by clear_exchange_caches() in main.py

        logger.info("WS WORKER START | %s symbol=%s", ex_id, symbol)

        while True:
            try:
                requested = self.orderbook_limits.get(ex_id, settings.orderbook_limit)
                first_limit, candidates = select_ob_limit(ex_id, requested)
                tried = []
                limits = [first_limit] + [
                    lim for lim in candidates if lim != first_limit
                ]
                if requested not in limits:
                    limits.append(requested)

                last_error = None
                for limit in limits:
                    tried.append(limit)
                    try:
                        try:
                            ob = await asyncio.wait_for(
                                ex.watch_order_book(symbol, limit=limit),
                                timeout=WS_WATCH_TIMEOUT_SECONDS,
                            )
                        except asyncio.TimeoutError as e:
                            last_error = e
                            logger.warning(
                                "WS TIMEOUT | %s %s timeout=%.3fs limit=%s",
                                ex_id,
                                symbol,
                                WS_WATCH_TIMEOUT_SECONDS,
                                limit,
                            )
                            raise
                        self.orderbook_limits[ex_id] = limit

                        if reconnect_attempts.get(symbol_key, 0) > 0:
                            logger.info(
                                "OB RECONNECT | %s %s limit=%s (recovered after %d attempts)",
                                ex_id,
                                symbol,
                                limit,
                                reconnect_attempts[symbol_key],
                            )
                        elif reconnect_attempts.get(symbol_key) == 0:
                            logger.info(
                                "OB LIMIT | %s %s limit=%s", ex_id, symbol, limit
                            )

                        on_update(ex_id, symbol, ob)
                        reconnect_attempts[symbol_key] = 0

                        break
                    except Exception as e:  # noqa: BLE001
                        message = str(e).lower()
                        last_error = e
                        if (
                            any(
                                token in message
                                for token in ("limit", "depth", "unsupported")
                            )
                            and limit != limits[-1]
                        ):
                            logger.info(
                                "OB RETRY | %s %s limit=%s next_limit=%s reason=unsupported",
                                ex_id,
                                symbol,
                                limit,
                                limits[min(len(tried), len(limits) - 1)],
                            )
                            continue
                        raise
                else:
                    if last_error:
                        raise last_error
            except Exception as e:
                if _is_non_retriable_ws_error_message(str(e)):
                    logger.warning(
                        "WS NONRETRIABLE | %s %s %s",
                        ex_id,
                        symbol,
                        str(e)[:160],
                    )
                    return

                attempt = reconnect_attempts.get(symbol_key, 0) + 1

                if _is_ccxt_client_keyerror(e):
                    logger.warning(
                        "WS CCXT BUG | %s %s KeyError in client cleanup, reconnecting...",
                        ex_id,
                        symbol,
                    )
                    delay = 2.0
                    reconnect_attempts[symbol_key] = attempt
                    await asyncio.sleep(delay)
                    continue

                if attempt > 10:
                    logger.warning(
                        "WS COOLDOWN | %s %s entering 5-min cooldown after %d failures",
                        ex_id,
                        symbol,
                        attempt,
                    )
                    await asyncio.sleep(300)
                    reconnect_attempts[symbol_key] = 0
                    continue

                delay = min(1.5 * (2 ** (attempt - 1)), 60.0)
                reconnect_attempts[symbol_key] = attempt
                self._log_retry("WS", f"{ex_id} {symbol}", str(e)[:100], attempt, delay)
                await asyncio.sleep(delay)

    async def _exchange_worker(
        self,
        ex_id: str,
        ex: Any,
        symbols: List[str],
        on_update: Callable[[str, str, Dict], None],
        reconnect_attempts: Dict[str | tuple[str, str], int],
    ) -> None:
        preload_attempt = 0
        while True:
            if await self._ensure_markets_loaded(ex_id, ex):
                break
            preload_attempt += 1
            delay = min(1.5 * (2 ** (preload_attempt - 1)), 60.0)
            logger.warning(
                "WS MARKETS RETRY | %s attempt=%d retry_in=%.1fs",
                ex_id,
                preload_attempt,
                delay,
            )
            await asyncio.sleep(delay)
        if (
            ex.has.get("watchOrderBookForSymbols", False)
            and ex_id not in BATCH_EXCLUDED_EXCHANGES
        ):
            await self._batch_worker(ex_id, ex, symbols, on_update, reconnect_attempts)
        else:
            logger.info(
                "WS LEGACY | %s using per-symbol mode (%d symbols)",
                ex_id,
                len(symbols),
            )
            if not symbols:
                logger.info(
                    "WS SKIP | %s no symbols allocated; not starting per-symbol workers",
                    ex_id,
                )
                return
            symbol_tasks = [
                asyncio.create_task(
                    self._symbol_worker(ex_id, ex, s, on_update, reconnect_attempts)
                )
                for s in symbols
            ]
            try:
                await asyncio.gather(*symbol_tasks)
            finally:
                for task in symbol_tasks:
                    task.cancel()
                await asyncio.gather(*symbol_tasks, return_exceptions=True)

    def start_exchange_worker(
        self,
        ex_id: str,
        ex: Any,
        symbols: List[str],
        on_update: Callable[[str, str, Dict], None],
    ) -> asyncio.Task:
        reconnect_attempts: Dict[str | tuple[str, str], int] = {}
        return asyncio.create_task(
            self._exchange_worker(ex_id, ex, symbols, on_update, reconnect_attempts)
        )

    async def subscribe_orderbooks(
        self,
        symbols_per_exchange: Dict[str, List[str]],
        on_update: Callable[[str, str, Dict], None],
    ):
        """Subscribe to orderbook updates for multiple symbols across exchanges.

        Uses batch API (watch_order_book_for_symbols) for exchanges that support it,
        with fallback to per-symbol subscriptions for others.
        """
        tasks = [
            self.start_exchange_worker(
                ex_id, ex, symbols_per_exchange.get(ex_id, []), on_update
            )
            for ex_id, ex in self.exchanges.items()
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        for ex_id, ex in self.exchanges.items():
            try:
                closer = getattr(ex, "close", None)
                if not closer:
                    continue
                result = closer()
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.warning("Failed to close WS exchange %s: %s", ex_id, e)


def select_ob_limit(ex_id: str, requested: int) -> tuple[int, List[int]]:
    candidates = SUPPORTED_ORDERBOOK_LIMITS.get(ex_id, [])
    if not candidates:
        return requested, [requested]
    if requested in candidates:
        return requested, candidates
    return candidates[0], candidates
