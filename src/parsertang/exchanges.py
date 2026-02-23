from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import ccxt

from parsertang.config import SUPPORTED_ORDERBOOK_LIMITS
from parsertang.health_monitor import CircuitOpenError, ExchangeHealthMonitor
from parsertang.utils.exchange_credentials import build_exchange_config

if TYPE_CHECKING:
    from parsertang.config import Settings

logger = logging.getLogger(__name__)


STABLE_QUOTES = {"USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "EURC"}


def select_orderbook_limit(ex_id: str, requested: int) -> int:
    """Select appropriate orderbook limit for exchange.

    Some exchanges (like KuCoin) only support specific limits.
    This function returns the closest supported limit.
    """
    # OKX: public REST supports deeper order books (e.g., 50/200), while our WS
    # pipeline is restricted to books5. WS limit selection is handled separately
    # in `streams.select_ob_limit()` and must remain conservative.
    if ex_id == "okx":
        return requested

    supported = SUPPORTED_ORDERBOOK_LIMITS.get(ex_id, [])
    if not supported:
        return requested

    if requested in supported:
        return requested

    # Find closest supported limit
    closest = min(supported, key=lambda x: abs(x - requested))
    return closest


class ExchangeGateway:
    def __init__(
        self,
        exchange_ids: List[str],
        settings: Settings,
        proxy_config: Dict | None = None,
        health_monitor: ExchangeHealthMonitor | None = None,
    ):
        """
        Initialize ExchangeGateway with authenticated exchanges.

        Args:
            exchange_ids: List of exchange IDs to initialize
            settings: Application settings containing API credentials
            proxy_config: Optional proxy configuration dict
            health_monitor: Optional circuit breaker monitor for fault tolerance
        """
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self._monitor = health_monitor
        for ex_id in exchange_ids:
            try:
                cls = getattr(ccxt, ex_id)

                # Build config with credentials using centralized builder
                config = build_exchange_config(ex_id, settings, proxy_config)

                # Log proxy status
                if proxy_config and any(proxy_config.values()):
                    logger.info("PROXY | %s REST API using proxy", ex_id)

                # Log authentication status
                if "apiKey" in config:
                    logger.info("AUTH | %s initialized with API credentials", ex_id)
                else:
                    logger.warning(
                        "AUTH | %s initialized WITHOUT credentials (may return limited data)",
                        ex_id,
                    )

                inst: ccxt.Exchange = cls(config)

                # HTX: Patch URLs directly since sync ccxt ignores hostname param
                # for fetch_currencies() and other endpoints. Singapore geo-block bypass.
                if ex_id == "htx" and inst.urls:
                    patched = 0
                    api_urls = inst.urls.get("api")
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
                                "HTX | Patched %d API URLs to api-aws.huobi.pro",
                                patched,
                            )

                self.exchanges[ex_id] = inst
            except Exception as e:
                logger.error("Failed to init exchange %s: %s", ex_id, e)

    def close_all(self) -> None:
        """Close all exchange connections.

        Note: Uses sync ccxt, so close() is called without await.
        """
        for ex_id, ex in self.exchanges.items():
            try:
                close_fn = getattr(ex, "close", None)
                if close_fn is not None:
                    close_fn()  # Sync ccxt uses sync close
                    logger.debug("Closed exchange: %s", ex_id)
            except Exception as e:
                logger.warning("Failed to close %s: %s", ex_id, e)

    def load_markets(self) -> Dict[str, Dict]:
        markets: Dict[str, Dict] = {}
        for ex_id, ex in self.exchanges.items():
            try:
                m = ex.load_markets()
                if not m:
                    logger.warning("load_markets returned empty for %s", ex_id)
                markets[ex_id] = m
            except Exception as e:
                logger.error("load_markets failed for %s: %s", ex_id, e)
        return markets

    @staticmethod
    def is_stable_quote(symbol: str) -> bool:
        try:
            base, quote = symbol.split("/")
        except ValueError:
            return False
        return quote.upper() in STABLE_QUOTES

    def fetch_order_book(
        self, ex_id: str, symbol: str, limit: int = 20
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Fetch order book with circuit breaker protection."""
        # Circuit breaker check
        if self._monitor and not self._monitor.is_available(ex_id):
            retry_after = self._monitor.get_retry_after(ex_id)
            raise CircuitOpenError(ex_id, retry_after)

        ex = self.exchanges[ex_id]
        try:
            # Use exchange-specific limit if needed
            actual_limit = select_orderbook_limit(ex_id, limit)
            ob = ex.fetch_order_book(symbol, limit=actual_limit)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])

            # Record success
            if self._monitor:
                self._monitor.record_success(ex_id)

            return bids, asks
        except Exception as e:
            # Record failure for circuit breaker
            if self._monitor:
                self._monitor.record_failure(ex_id, e)
            raise

    def fetch_ticker(self, ex_id: str, symbol: str) -> Tuple[float, float]:
        """Fetch ticker with circuit breaker protection."""
        # Circuit breaker check
        if self._monitor and not self._monitor.is_available(ex_id):
            retry_after = self._monitor.get_retry_after(ex_id)
            raise CircuitOpenError(ex_id, retry_after)

        ex = self.exchanges[ex_id]
        try:
            t = ex.fetch_ticker(symbol)

            # Record success
            if self._monitor:
                self._monitor.record_success(ex_id)

            return t.get("bid", 0.0), t.get("ask", 0.0)
        except Exception as e:
            # Record failure for circuit breaker
            if self._monitor:
                self._monitor.record_failure(ex_id, e)
            raise

    def fetch_currencies(self, ex_id: str) -> Dict[str, Any]:
        """Fetch currencies with circuit breaker protection."""
        # Circuit breaker check
        if self._monitor and not self._monitor.is_available(ex_id):
            retry_after = self._monitor.get_retry_after(ex_id)
            raise CircuitOpenError(ex_id, retry_after)

        ex = self.exchanges[ex_id]
        try:
            result = ex.fetch_currencies()

            # Record success
            if self._monitor:
                self._monitor.record_success(ex_id)

            return result or {}
        except Exception as e:
            # Record failure for circuit breaker
            if self._monitor:
                self._monitor.record_failure(ex_id, e)
            raise

    def get_health_summary(self) -> Dict[str, Any]:
        """Get circuit breaker health status for all exchanges."""
        if self._monitor:
            return self._monitor.get_health_summary()
        return {}

    def fetch_fees(self, ex_id: str) -> Dict:
        ex = self.exchanges[ex_id]
        try:
            # Try fetch_trading_fees first (standard for many exchanges)
            if hasattr(ex, "fetch_trading_fees"):
                return ex.fetch_trading_fees()
            # Fallback to fetch_fees if available as alias
            if hasattr(ex, "fetch_fees"):
                return ex.fetch_fees()
            return {}
        except Exception as e:
            # Use debug logging for known missing methods to reduce noise
            if "'fetchFundingFees'" in str(e):
                logger.debug(
                    "Exchange %s does not support fetch_fees (using default)", ex_id
                )
            else:
                logger.warning("Failed to fetch fees for %s: %s", ex_id, e)
            return {}
