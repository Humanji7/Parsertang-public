"""
Dynamic withdrawal fee fetching and caching.

This module implements SPEC-FEE-001 Phase 1: Foundation
for dynamic withdrawal fee management from exchange APIs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from parsertang.network_aliases import normalize_network
from parsertang.static_withdrawal_fees import check_fee_drift, get_fallback_fee

try:
    import ccxt.pro  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover
    # Allows running unit tests in minimal environments without ccxt installed.
    # Production environments are expected to have ccxt.pro available.
    ccxt = None  # type: ignore[assignment]


CURRENCY_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    # Observed in live: APT/USDT uses base_currency=APT, but withdrawal fees can be keyed as APTOS.
    "APT": ("APTOS",),
}

# Rate limiting for repetitive log messages (FEE LOOKUP MISMATCH spam prevention)
# Key: (exchange_id, currency, frozenset(networks)) -> last_log_time
_log_rate_limit_cache: Dict[Tuple[str, str, frozenset], float] = {}
LOG_RATE_LIMIT_SECONDS = 300.0  # Log same mismatch once per 5 minutes


def canonical_currency_codes(exchange: Any, currency_code: str) -> tuple[str, ...]:
    """Return possible canonical currency codes for fee-cache indexing.

    Some exchanges expose different currency codes across endpoints:
    - markets/symbols might use one code (e.g. "APT")
    - deposit/withdraw fee endpoints might use another (e.g. "APTOS")

    We store fees under both the raw code and, when available, the exchange's
    canonical code via `safe_currency_code`.
    """
    raw = (currency_code or "").strip().upper()
    if not raw:
        return tuple()

    codes: list[str] = [raw]

    safe_fn = getattr(exchange, "safe_currency_code", None)
    if callable(safe_fn):
        try:
            safe = safe_fn(raw)
            safe_norm = (str(safe) if safe else "").strip().upper()
            if safe_norm and safe_norm not in codes:
                codes.append(safe_norm)
        except Exception:  # noqa: BLE001
            pass

    return tuple(codes)


logger = logging.getLogger(__name__)


def normalize_network_code(network: str, currency: str = "") -> str:
    """Normalize a network code to the canonical form used by fee cache.

    Used by both cached fees and live (on-demand) validation to avoid false
    mismatches like TRX vs TRC20 or suffix variants like TRC20-USDT.
    """
    raw = (network or "").strip().upper()
    if not raw:
        return ""
    if currency:
        curr = currency.strip().upper()
        if curr:
            if raw.endswith(f"-{curr}"):
                raw = raw[: -len(curr) - 1]
            if raw.startswith(f"{curr}-"):
                raw = raw[len(curr) + 1 :]
    normalized = normalize_network(raw)
    return normalized if normalized else raw


def extract_withdraw_fee_from_currencies(
    currencies: dict[str, Any],
    *,
    currency: str,
    network: str,
) -> float | None:
    """Extract withdraw fee (base currency units) from ccxt fetch_currencies() payload."""
    if not currencies:
        return None
    cur = (currency or "").strip().upper()
    if not cur:
        return None
    net = normalize_network_code(network, cur)
    if not net:
        return None

    info = currencies.get(cur) or {}
    networks = (info.get("networks") or {}) if isinstance(info, dict) else {}
    if not isinstance(networks, dict):
        return None

    for raw_net, net_info in networks.items():
        if not isinstance(net_info, dict):
            continue
        if not net_info.get("withdraw", False):
            continue
        if not net_info.get("active", False):
            continue
        fee = net_info.get("fee")
        if fee is None:
            continue
        normalized = normalize_network_code(str(raw_net), cur)
        if normalized != net:
            continue
        try:
            fee_f = float(fee)
        except (TypeError, ValueError):
            continue
        if fee_f < 0:
            continue
        return fee_f

    return None


def extract_withdraw_fee_from_deposit_withdraw_fees(
    fees: dict[str, Any],
    *,
    currency: str,
    network: str,
) -> float | None:
    """Extract withdraw fee (base currency units) from ccxt fetch_deposit_withdraw_fees() payload."""
    if not fees:
        return None
    cur = (currency or "").strip().upper()
    if not cur:
        return None
    net = normalize_network_code(network, cur)
    if not net:
        return None

    cur_info = fees.get(cur) or {}
    networks = (cur_info.get("networks") or {}) if isinstance(cur_info, dict) else {}
    if not isinstance(networks, dict):
        return None

    for raw_net, net_info in networks.items():
        if not isinstance(net_info, dict):
            continue
        withdraw_info = net_info.get("withdraw") or {}
        if not isinstance(withdraw_info, dict):
            continue
        fee = withdraw_info.get("fee")
        if fee is None:
            continue
        normalized = normalize_network_code(str(raw_net), cur)
        if normalized != net:
            continue
        try:
            fee_f = float(fee)
        except (TypeError, ValueError):
            continue
        if fee_f < 0:
            continue
        return fee_f

    return None


def fetch_withdraw_fee_live(
    exchange: Any,
    *,
    currency: str,
    network: str,
) -> tuple[float | None, str]:
    """Fetch withdrawal fee for (currency, network) on-demand using exchange API.

    Returns:
        (fee_base, source) where:
        - fee_base: fee in base currency units, 0.0 is valid, None if unknown/unavailable
        - source: "currencies" | "deposit_withdraw_fees" | "error"

    Designed for slow-path validation (pre-alert gate, ALERTTRUTH), not hot-path WS.
    """
    try:
        currencies = exchange.fetch_currencies() or {}
        fee = extract_withdraw_fee_from_currencies(
            currencies,
            currency=currency,
            network=network,
        )
        if fee is not None:
            return fee, "currencies"
    except Exception:
        pass

    try:
        fn = getattr(exchange, "fetch_deposit_withdraw_fees", None)
        if callable(fn):
            fees = fn() or {}
            fee = extract_withdraw_fee_from_deposit_withdraw_fees(
                fees,
                currency=currency,
                network=network,
            )
            if fee is not None:
                return fee, "deposit_withdraw_fees"
    except Exception:
        pass

    return None, "error"


@dataclass
class WithdrawalFeeCache:
    """
    Cache entry for withdrawal fees.

    Structure: exchange_id → currency → network → fee_usd
    """

    fees: Dict[str, Dict[str, Dict[str, float]]]
    last_updated: float
    cache_lifetime: float = 3600.0  # 1 hour default

    def is_stale(self) -> bool:
        """Check if cache has expired."""
        return (time.time() - self.last_updated) >= self.cache_lifetime

    def get_fee(
        self,
        exchange_id: str,
        currency: str,
        network: str,
    ) -> Optional[float]:
        """
        Get withdrawal fee for specific exchange, currency, and network.

        Args:
            exchange_id: Exchange ID (e.g., "bybit")
            currency: Currency code (e.g., "USDT")
            network: Network code (e.g., "TRC20")

        Returns:
            Withdrawal fee in USD or None if not found
        """
        return self.fees.get(exchange_id, {}).get(currency, {}).get(network)


class WithdrawalFeeManager:
    """
    Manages dynamic withdrawal fee fetching and caching.

    This class fetches withdrawal fees from exchange APIs, caches them,
    and provides lookup methods. Returns 0.0 for missing fees.
    """

    def __init__(
        self,
        exchanges: Dict[str, Any],
        cache_lifetime: float = 3600.0,
        fetch_timeout: float = 10.0,
    ):
        """
        Initialize fee manager.

        Args:
            exchanges: Dict of exchange_id → ccxt.Exchange instance
            cache_lifetime: Cache lifetime in seconds (default: 1 hour)
            fetch_timeout: API fetch timeout in seconds (default: 10s)
        """
        self.exchanges = exchanges
        self.cache_lifetime = cache_lifetime
        self.fetch_timeout = fetch_timeout
        self.cache: Optional[WithdrawalFeeCache] = None
        self._refresh_task: Optional[asyncio.Task] = None

    @staticmethod
    def _normalize_network_code(network: str, currency: str = "") -> str:
        """
        Normalize network code to canonical form.

        Handles variations like:
        - "TRC20-USDT" → "TRC20"
        - "USDT-TRC20" → "TRC20"
        - "TRX" → "TRC20" (exchange-specific alias)
        - "ETH" → "ERC20" (exchange-specific alias)
        - "BSC" → "BEP20" (exchange-specific alias)

        Args:
            network: Raw network code from exchange
            currency: Currency code (for suffix removal)

        Returns:
            Normalized network code
        """
        # Remove whitespace
        network = network.strip().upper()

        # Remove currency suffix (handles both "TRC20-USDT" and "USDT-TRC20")
        if currency:
            currency = currency.upper()
            # Remove trailing currency: "TRC20-USDT" → "TRC20"
            if network.endswith(f"-{currency}"):
                network = network[: -len(currency) - 1]
            # Remove leading currency: "USDT-TRC20" → "TRC20"
            if network.startswith(f"{currency}-"):
                network = network[len(currency) + 1 :]

        # Use centralized network normalization
        normalized = normalize_network(network)
        return normalized if normalized else network

    async def _fetch_exchange_fees(
        self,
        exchange_id: str,
        exchange: Any,
    ) -> Dict[str, Dict[str, float]]:
        """
        Fetch withdrawal fees from a single exchange.

        Args:
            exchange_id: Exchange ID (for logging)
            exchange: ccxt.Exchange instance

        Returns:
            Dict: currency → network → fee_usd
        """
        try:
            logger.info(f"FEE FETCH | {exchange_id}: fetching currencies...")

            # Fetch currencies with timeout
            currencies_task = exchange.fetch_currencies()
            currencies = await asyncio.wait_for(
                currencies_task,
                timeout=self.fetch_timeout,
            )

            result: Dict[str, Dict[str, float]] = {}
            network_count = 0

            for currency_code, currency_info in (currencies or {}).items():
                networks = (currency_info or {}).get("networks", {}) or {}

                for network_code, network_info in networks.items():
                    # Skip if withdrawal is disabled
                    # Note: network_info["withdraw"] is a boolean in ccxt.pro
                    if not network_info.get("withdraw", False):
                        continue

                    # Skip if network is inactive
                    if not network_info.get("active", False):
                        continue

                    # Get withdrawal fee (directly from network_info)
                    # NOTE: fee=0.0 is VALID (free withdrawal), fee=None is unknown
                    fee = network_info.get("fee")
                    if fee is None:
                        continue

                    # Normalize network code
                    normalized_network = self._normalize_network_code(
                        network_code,
                        currency_code,
                    )

                    # Store fee
                    for canonical in canonical_currency_codes(exchange, currency_code):
                        if canonical not in result:
                            result[canonical] = {}
                        result[canonical][normalized_network] = float(fee)
                        network_count += 1

            # Gate.io/MEXC fallback: if no networks found, try alternative API
            # Gate.io: only if network_count == 0 (fetch_currencies returns null fees)
            # MEXC: always (fetch_currencies returns partial data ~53% coverage)
            should_fallback = (
                (exchange_id == "gate" and network_count == 0)
                or (exchange_id == "bybit")
                # MEXC fetch_currencies is partial; fallback improves coverage.
                or (exchange_id == "mexc" and currencies and len(currencies) > 0)
                # Some exchanges provide incomplete/none withdrawal fees via fetch_currencies
                # for specific assets; merge fallback to avoid per-asset gaps (no_fee_data).
                or (
                    exchange_id in {"okx", "kucoin"}
                    and currencies
                    and len(currencies) > 0
                )
            )

            if should_fallback:
                if exchange_id == "mexc":
                    logger.warning(
                        f"FEE FETCH | {exchange_id}: using fetch_deposit_withdraw_fees() "
                        f"for better coverage (fetch_currencies returns partial data ~53%)"
                    )
                else:
                    logger.warning(
                        f"FEE FETCH | {exchange_id}: no fees from fetch_currencies(), "
                        f"trying fetch_deposit_withdraw_fees() fallback..."
                    )
                fallback = await self._fetch_fees_fallback(exchange_id, exchange)

                # Merge strategy:
                # - Keep whatever fetch_currencies provided.
                # - Overlay fallback data (usually more complete for gate/mexc/bybit).
                # This avoids regressions where fallback is missing a currency/network
                # that fetch_currencies actually had.
                if fallback:
                    for currency_code, networks in fallback.items():
                        if currency_code not in result:
                            result[currency_code] = {}
                        result[currency_code].update(networks)

                return result

            logger.info(
                f"FEE FETCH OK | {exchange_id}: "
                f"{len(result)} currencies, {network_count} networks"
            )
            return result

        except asyncio.TimeoutError:
            logger.error(
                f"FEE FETCH FAIL | {exchange_id}: timeout after {self.fetch_timeout}s"
            )
            return {}
        except Exception as e:
            logger.error(f"FEE FETCH FAIL | {exchange_id}: {e}")
            return {}

    async def _fetch_fees_fallback(
        self,
        exchange_id: str,
        exchange: Any,
        max_retries: int = 3,
    ) -> Dict[str, Dict[str, float]]:
        """
        Fallback method for Gate.io and MEXC using fetch_deposit_withdraw_fees().

        Both exchanges return fee=null or partial data in fetch_currencies(), but provide
        proper fees via fetch_deposit_withdraw_fees() endpoint.

        Implements exponential backoff retry strategy for timeout handling.

        Args:
            exchange_id: Exchange ID (e.g., "gate", "mexc")
            exchange: ccxt.Exchange instance
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            Dict: currency → network → fee_usd
        """
        # Exchange-specific base timeouts
        # Gate.io: 10s base (historically slow, needs retries)
        # MEXC: 15s base (8555 currencies, but typically fast ~14s observed)
        timeout_base_map = {
            "gate": 15.0,
            "bybit": 15.0,
            "mexc": 30.0,
            "okx": 15.0,
            "kucoin": 20.0,
        }

        timeout_base = timeout_base_map.get(exchange_id, 10.0)
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = 2**attempt  # Exponential backoff: 2s, 4s, 8s
                    logger.info(
                        f"FEE FETCH FALLBACK RETRY | {exchange_id}: "
                        f"attempt {attempt + 1}/{max_retries} after {delay}s delay"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.info(
                        f"FEE FETCH FALLBACK | {exchange_id}: "
                        f"using fetch_deposit_withdraw_fees()..."
                    )

                # Fetch deposit/withdraw fees with extended timeout for retries
                # Use exchange-specific base timeout with exponential growth
                timeout = timeout_base * (1.5**attempt)
                fees_task = exchange.fetch_deposit_withdraw_fees()
                fees_data = await asyncio.wait_for(
                    fees_task,
                    timeout=timeout,
                )

                result: Dict[str, Dict[str, float]] = {}
                network_count = 0

                if not fees_data or not isinstance(fees_data, dict):
                    return result

                # Cast to proper type for iteration
                fees_dict: Dict[str, Dict] = fees_data  # type: ignore[assignment]
                for currency_code, currency_info in fees_dict.items():  # type: ignore[misc]
                    networks = currency_info.get("networks", {})  # type: ignore[union-attr]
                    if not networks or not isinstance(networks, dict):
                        continue

                    for network_code, network_info in networks.items():
                        # Extract withdraw info (Gate.io/MEXC structure: networks.TRC20.withdraw.fee)
                        withdraw_info = (
                            network_info.get("withdraw", {})
                            if isinstance(network_info, dict)
                            else {}
                        )
                        if not isinstance(withdraw_info, dict):
                            continue

                        # Get withdrawal fee
                        # NOTE: fee=0.0 is VALID (free withdrawal), fee=None is unknown
                        fee = withdraw_info.get("fee")
                        if fee is None:
                            continue

                        # Normalize network code
                        normalized_network = self._normalize_network_code(
                            network_code,
                            currency_code,
                        )

                        # Store fee
                        for canonical in canonical_currency_codes(
                            exchange, currency_code
                        ):
                            if canonical not in result:
                                result[canonical] = {}
                            result[canonical][normalized_network] = float(fee)
                            network_count += 1

                logger.info(
                    f"FEE FETCH FALLBACK OK | {exchange_id}: "
                    f"{len(result)} currencies, {network_count} networks (attempt {attempt + 1})"
                )
                return result

            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    logger.error(
                        f"FEE FETCH FALLBACK FAIL | {exchange_id}: "
                        f"timeout after {max_retries} attempts"
                    )
                    return {}
                logger.warning(
                    f"FEE FETCH FALLBACK TIMEOUT | {exchange_id}: "
                    f"attempt {attempt + 1} timed out, retrying..."
                )
            except Exception as e:
                logger.error(f"FEE FETCH FALLBACK FAIL | {exchange_id}: {e}")
                return {}

        return {}

    async def fetch_all_fees(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Fetch withdrawal fees from all exchanges.

        Returns:
            Dict: exchange_id → currency → network → fee_usd
        """
        logger.info("FEE FETCH | Starting fetch from all exchanges...")

        tasks = [
            self._fetch_exchange_fees(exchange_id, exchange)
            for exchange_id, exchange in self.exchanges.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Combine results
        all_fees: Dict[str, Dict[str, Dict[str, float]]] = {}
        for exchange_id, fees in zip(self.exchanges.keys(), results):
            all_fees[exchange_id] = fees

        # Count total networks across all exchanges
        total_networks = sum(
            sum(len(networks) for networks in exchange_fees.values())
            for exchange_fees in all_fees.values()
        )

        logger.info(
            f"FEE FETCH COMPLETE | "
            f"{len(all_fees)} exchanges, {total_networks} total networks"
        )

        return all_fees

    async def refresh_cache(self) -> None:
        """
        Refresh fee cache from APIs.

        Side effects:
            - Updates self.cache
            - Logs success/failure
        """
        logger.info("FEE CACHE | Starting refresh...")

        fetched = await self.fetch_all_fees()

        # Merge with previous cache to avoid dropping to empty on transient fetch failures.
        previous = self.cache.fees if self.cache else {}
        merged: Dict[str, Dict[str, Dict[str, float]]] = {}
        updated_exchanges: list[str] = []
        kept_exchanges: list[str] = []

        exchange_ids = sorted(set(self.exchanges.keys()) | set(fetched.keys()) | set(previous.keys()))
        for exchange_id in exchange_ids:
            new_fees = fetched.get(exchange_id, {})
            if new_fees:
                merged[exchange_id] = new_fees
                updated_exchanges.append(exchange_id)
                continue

            old_fees = previous.get(exchange_id, {})
            if old_fees:
                merged[exchange_id] = old_fees
                kept_exchanges.append(exchange_id)
            else:
                merged[exchange_id] = {}

        # Count total networks across all exchanges (merged view).
        total_networks = sum(
            sum(len(networks) for networks in exchange_fees.values())
            for exchange_fees in merged.values()
        )
        exchanges_with_data = sum(1 for f in merged.values() if f)

        # If we have absolutely no data, retry sooner (common on cold-start when APIs time out).
        effective_lifetime = (
            self.cache_lifetime if total_networks > 0 else min(self.cache_lifetime, 300)
        )

        self.cache = WithdrawalFeeCache(
            fees=merged,
            last_updated=time.time(),
            cache_lifetime=effective_lifetime,
        )

        logger.info(
            "FEE CACHE OK | Cached fees for %d exchanges, next refresh in %d minutes (updated=%s kept=%s total_networks=%d)",
            exchanges_with_data,
            int(effective_lifetime / 60),
            ",".join(updated_exchanges) or "-",
            ",".join(kept_exchanges) or "-",
            total_networks,
        )

        # Check for fee drift between static fallback and dynamic API (only if we actually refreshed something).
        if updated_exchanges:
            drifts = check_fee_drift(merged)
            for drift in drifts:
                logger.error(
                    f"FEE DRIFT DETECTED | {drift['exchange']} {drift['currency']}/{drift['network']}: "
                    f"fallback=${drift['fallback_fee']:.4f} vs API=${drift['dynamic_fee']:.4f} "
                    f"({drift['drift_pct']:.1f}% drift)"
                )

    async def _refresh_loop(self) -> None:
        """Background loop to refresh cache."""
        while True:
            try:
                # Wait for cache to become stale
                if self.cache and not self.cache.is_stale():
                    sleep_time = self.cache.cache_lifetime - (
                        time.time() - self.cache.last_updated
                    )
                    await asyncio.sleep(max(60, sleep_time))
                    continue

                # Refresh cache
                await self.refresh_cache()

            except asyncio.CancelledError:
                logger.info("FEE CACHE | Background refresh stopped")
                break
            except Exception as e:
                logger.error(f"FEE CACHE ERROR | {e}, retrying in 5 minutes")
                await asyncio.sleep(300)  # 5 minutes

    async def start_background_refresh(self) -> None:
        """
        Start background task to refresh cache periodically.

        Side effects:
            - Creates asyncio.Task stored in self._refresh_task
            - Initial fetch happens immediately
            - Background loop continues until stopped
        """
        # Initial fetch
        await self.refresh_cache()

        # Start background loop
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        logger.info("FEE CACHE | Background refresh started")

    async def stop_background_refresh(self) -> None:
        """
        Stop background refresh task.

        Side effects:
            - Cancels self._refresh_task
            - Waits for task to finish
        """
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
            logger.info("FEE CACHE | Background refresh stopped")

    def get_withdrawal_fee(
        self,
        exchange_id: str,
        currency: str,
        network: str,
    ) -> tuple[float | None, str]:
        """
        Get withdrawal fee for specific exchange, currency, and network.

        Args:
            exchange_id: Exchange ID (e.g., "bybit")
            currency: Currency code (e.g., "USDT")
            network: Network code (e.g., "TRC20")

        Returns:
            Tuple of (fee, confidence):
            - fee: Withdrawal fee in base currency, 0.0 for free, or None if not found
            - confidence: "HIGH" (from API cache), "MEDIUM" (fallback), or "LOW"

        Logs:
            DEBUG: Cache hit with fee amount
            DEBUG: Missing from cache
        """
        # Race condition protection: snapshot cache reference at start
        cache_snapshot = self.cache
        if not cache_snapshot:
            logger.debug(
                f"FEE CACHE MISS | {exchange_id} {currency}/{network}: no cache"
            )
            return None, "HIGH"

        # Strategy: Try multiple lookup approaches to handle normalization mismatches
        # between how fees are stored vs how they're looked up.

        currencies_to_try = [currency]
        # Add currency aliases (e.g., APT → APTOS)
        currencies_to_try.extend(CURRENCY_CODE_ALIASES.get(currency.upper(), ()))

        for curr in currencies_to_try:
            # 1. Exact match
            cached_fee = cache_snapshot.get_fee(exchange_id, curr, network)
            if cached_fee is not None:
                logger.debug(
                    f"FEE CACHE HIT | {exchange_id} {curr}/{network}: "
                    f"${cached_fee:.4f}"
                    + (f" (via alias {currency}→{curr})" if curr != currency else "")
                )
                return cached_fee, "HIGH"

            # 2. Try normalized version of the requested network
            normalized_net = normalize_network(network)
            if normalized_net and normalized_net != network:
                cached_fee = cache_snapshot.get_fee(exchange_id, curr, normalized_net)
                if cached_fee is not None:
                    logger.debug(
                        f"FEE CACHE HIT (NORM NET) | {exchange_id} {curr}/{network}→{normalized_net}: "
                        f"${cached_fee:.4f}"
                    )
                    return cached_fee, "HIGH"

            # 3. Search cached networks that normalize to match the requested network
            # This handles the reverse case: cache has "APTOS" but we're looking for "APT"
            currency_fees = cache_snapshot.fees.get(exchange_id, {}).get(curr, {})
            requested_normalized = normalize_network(network) or network
            for cached_net, fee in currency_fees.items():
                cached_normalized = normalize_network(cached_net) or cached_net
                if cached_normalized == requested_normalized and fee is not None:
                    logger.debug(
                        f"FEE CACHE HIT (REVERSE NORM) | {exchange_id} {curr}/{network}: "
                        f"found via {cached_net} (both normalize to {cached_normalized}), "
                        f"${fee:.4f}"
                    )
                    return fee, "HIGH"

        # No match found in cache - try static fallback
        # IMPORTANT: Only use fallback if exchange has some data in cache
        # (API is responding). Empty exchange cache = API down, don't fallback.
        exchange_cache = cache_snapshot.fees.get(exchange_id, {})
        if exchange_cache:
            # Exchange has data, but this currency/network is missing - use fallback
            fallback = get_fallback_fee(exchange_id, currency, network)
            if fallback:
                logger.info(
                    f"FEE FALLBACK | {exchange_id} {currency}/{network}: "
                    f"${fallback.fee:.4f} (source: {fallback.source}, "
                    f"verified: {fallback.verified_at})"
                )
                return fallback.fee, "MEDIUM"

        # No match found - return None (not 0.0, which means free withdrawal)
        logger.debug(
            f"FEE CACHE MISS | {exchange_id} {currency}/{network}: " f"not in cache"
        )
        return None, "HIGH"

    def get_per_exchange_fees_usd(
        self,
        exchange_id: str,
        currency: str,
        networks: List[str],
        current_price_usd: float,
    ) -> Dict[str, float]:
        """
        Get fees for all networks on a specific exchange, converted to USD.

        BUG FIX #2: Fees are returned in BASE CURRENCY from the API, but must be
        converted to USD for proper network comparison. Comparing 0.001 BTC to
        0.1 LTC is invalid without conversion.

        Used by pick_best_network() to compare networks in a common currency.

        Args:
            exchange_id: Exchange ID
            currency: Currency code
            networks: List of network codes to check
            current_price_usd: Current price of the currency in USD (for conversion)

        Returns:
            Dict of network → fee_usd (converted to USD for proper comparison)
        """
        result: Dict[str, float] = {}

        # DEBUG: Log cache keys vs requested networks for diagnosis (rate-limited)
        if self.cache:
            # Check all possible currency keys (including aliases)
            cache_keys_primary = list(
                self.cache.fees.get(exchange_id, {}).get(currency, {}).keys()
            )
            cache_keys_aliases = []
            for alias in CURRENCY_CODE_ALIASES.get(currency.upper(), ()):
                alias_keys = list(
                    self.cache.fees.get(exchange_id, {}).get(alias, {}).keys()
                )
                if alias_keys:
                    cache_keys_aliases.append(f"{alias}:{alias_keys}")

            # Rate limit repetitive mismatch logs
            rate_limit_key = (exchange_id, currency, frozenset(networks))
            now = time.time()
            last_log = _log_rate_limit_cache.get(rate_limit_key, 0.0)
            should_log = (now - last_log) >= LOG_RATE_LIMIT_SECONDS

            if not cache_keys_primary and not cache_keys_aliases:
                if should_log:
                    _log_rate_limit_cache[rate_limit_key] = now
                    logger.warning(
                        "FEE LOOKUP MISS | %s %s has NO cache keys (requested=%s)",
                        exchange_id,
                        currency,
                        networks,
                    )
            elif not any(n in cache_keys_primary for n in networks):
                if should_log:
                    _log_rate_limit_cache[rate_limit_key] = now
                    logger.warning(
                        "FEE LOOKUP MISMATCH | %s %s requested=%s cache_keys=%s aliases=%s",
                        exchange_id,
                        currency,
                        networks,
                        cache_keys_primary,
                        cache_keys_aliases or "none",
                    )

        for network in networks:
            # get_withdrawal_fee returns fee in BASE CURRENCY (e.g., 0.1 LTC)
            # Returns (None, confidence) if not found, (0.0, confidence) if free withdrawal
            fee_base, _confidence = self.get_withdrawal_fee(
                exchange_id, currency, network
            )
            if fee_base is not None:  # Include 0.0 (free withdrawal)
                # Convert to USD for proper comparison across different currencies
                fee_usd = fee_base * current_price_usd
                result[network] = fee_usd
                logger.debug(
                    f"FEE NETWORK CONVERSION | {exchange_id} {currency}/{network}: "
                    f"{fee_base:.6f} {currency} * ${current_price_usd:.2f} = ${fee_usd:.4f} USD"
                )

        return result
