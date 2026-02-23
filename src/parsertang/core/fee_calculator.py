"""Fee calculation and network selection for arbitrage opportunities.

This module provides pure functions for:
- Calculating trading fees (buy/sell taker fees)
- Selecting optimal withdrawal network (cheapest fees)
- Computing net profit after all fees
- Validating fee data availability

CRITICAL DESIGN NOTES:
- Functions accept state via AppState parameter (dependency injection)
- No global variables — all state passed explicitly
- Returns immutable FeeCalculationResult dataclass
- Handles fee debug logging with rate limiting

IMPORTANT: Read docs/fees-critical.md before modifying this module.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from parsertang.arbitrage import compute_net_profit_pct
from parsertang.config import settings
from parsertang.exchange_fees import get_taker_fee
from parsertang.fee_debug import (
    RateLimiter,
    is_fee_debug_enabled,
    parse_debug_fee_symbols,
)
from parsertang.network_aliases import normalize_network
from parsertang.networks import pick_best_network, resolve_network_for_token
from parsertang.static_withdrawal_fees import get_fallback_networks, has_fallback_data

if TYPE_CHECKING:
    from parsertang.core.state_manager import AppState

logger = logging.getLogger(__name__)

# Debug configuration (module-level constants)
_DEBUG_FEE_SYMBOLS = parse_debug_fee_symbols(settings.debug_fee_symbols)
_FEE_DEBUG_LIMITER = RateLimiter(
    interval_seconds=float(max(settings.debug_fee_log_interval_seconds, 0))
)


@dataclass
class FeeCalculationResult:
    """Result of fee calculation and network selection for an arbitrage opportunity.

    Attributes:
        network: Selected network for withdrawal (e.g., 'TRC20', 'BEP20', 'SOL')
        withdraw_fee_base: Withdrawal fee in BASE CURRENCY (e.g., 0.0069 LTC)
        buy_fee_pct: Taker fee percentage on buy exchange
        sell_fee_pct: Taker fee percentage on sell exchange
        withdraw_fee_pct: Withdrawal fee as percentage of trade amount
        net_profit_pct: Net profit percentage after all fees
        error_reason: Error reason if calculation failed, None if successful
        fee_confidence: Confidence level for withdrawal fee (HIGH/MEDIUM/LOW)
    """

    network: str | None
    """Selected network for withdrawal (e.g., 'TRC20', 'BEP20', 'SOL')."""

    withdraw_fee_base: float
    """Withdrawal fee in BASE CURRENCY (e.g., 0.0069 LTC)."""

    buy_fee_pct: float
    """Taker fee percentage on buy exchange."""

    sell_fee_pct: float
    """Taker fee percentage on sell exchange."""

    withdraw_fee_pct: float
    """Withdrawal fee as percentage of trade amount."""

    net_profit_pct: float
    """Net profit percentage after all fees."""

    net_profit_funded_pct: float = 0.0
    """Net profit percentage for funded-arb (trade fees only; withdrawal excluded)."""

    error_reason: str | None = None
    """Error reason if calculation failed, None if successful."""

    fee_confidence: str = "HIGH"
    """Confidence level for withdrawal fee: HIGH (dynamic), MEDIUM (static fallback), LOW (assumed 0)."""

    @property
    def is_valid(self) -> bool:
        """Check if calculation succeeded (no error)."""
        return self.error_reason is None

    @property
    def total_fees_pct(self) -> float:
        """Total fees percentage (buy + sell + withdrawal)."""
        return self.buy_fee_pct + self.sell_fee_pct + self.withdraw_fee_pct


def calculate_opportunity_fees_and_network(
    symbol: str,
    buy_exchange: str,
    sell_exchange: str,
    best_ask: float,
    best_bid: float,
    state: AppState,
) -> FeeCalculationResult:
    """Calculate fees and select optimal network for an arbitrage opportunity.

    Extracts common logic for network selection, fee calculation, and net profit computation.
    Used by both evaluate_arbitrage_for_symbol() and initial REST snapshot in main().

    CRITICAL: Reads currency_cache and fee_manager from state WITHOUT locks.
    This is safe because:
    - currency_cache is populated at startup and refreshed periodically (low contention)
    - fee_manager is read-only after initialization
    - This function is called from sync context (WS callback) where async locks would block

    Args:
        symbol: Trading pair (e.g., "LTC/USDT")
        buy_exchange: Exchange to buy from
        sell_exchange: Exchange to sell on
        best_ask: Buy price (best ask)
        best_bid: Sell price (best bid)
        state: Application state containing currency_cache and fee_manager

    Returns:
        FeeCalculationResult containing network, fees, and validation status
        - is_valid: True if calculation succeeded
        - error_reason: None if validation passed, otherwise contains error code:
          - "invalid_symbol": Symbol parsing failed
          - "currency_not_available_buy": Base currency missing from buy exchange API
          - "currency_not_available_sell": Base currency missing from sell exchange API
          - "no_fee_data": No withdrawal fee information available
          - "no_valid_networks": Common networks exist but none have fee data
          - "invalid_withdrawal_fee": Withdrawal fee is zero or negative
    """
    # Get taker fees
    buy_fee_pct = get_taker_fee(buy_exchange)
    sell_fee_pct = get_taker_fee(sell_exchange)

    # Extract base currency
    base_currency = symbol.split("/")[0]

    # Get currency metadata (direct access — no lock needed for read)
    buy_curr = state.currency_cache.get(buy_exchange, {}).get(base_currency, {})
    sell_curr = state.currency_cache.get(sell_exchange, {}).get(base_currency, {})

    # Flags for fallback network usage
    use_fallback_networks_buy = False
    use_fallback_networks_sell = False

    # FR1: Currency availability validation (with fallback support)
    if not buy_curr:
        if has_fallback_data(buy_exchange, base_currency):
            # Fallback data exists — continue with fallback networks
            use_fallback_networks_buy = True
            logger.info(
                "CURRENCY FALLBACK | %s buy=%s (using static fallback data)",
                symbol,
                buy_exchange,
            )
        else:
            logger.warning(
                "CURRENCY NOT AVAILABLE | %s buy=%s (not in API, no fallback)",
                symbol,
                buy_exchange,
            )
            return FeeCalculationResult(
                network=None,
                withdraw_fee_base=0.0,
                buy_fee_pct=buy_fee_pct,
                sell_fee_pct=sell_fee_pct,
                withdraw_fee_pct=0.0,
                net_profit_pct=0.0,
                error_reason="currency_not_available_buy",
            )

    if not sell_curr:
        if has_fallback_data(sell_exchange, base_currency):
            # Fallback data exists — continue with fallback networks
            use_fallback_networks_sell = True
            logger.info(
                "CURRENCY FALLBACK | %s sell=%s (using static fallback data)",
                symbol,
                sell_exchange,
            )
        else:
            logger.warning(
                "CURRENCY NOT AVAILABLE | %s sell=%s (not in API, no fallback)",
                symbol,
                sell_exchange,
            )
            return FeeCalculationResult(
                network=None,
                withdraw_fee_base=0.0,
                buy_fee_pct=buy_fee_pct,
                sell_fee_pct=sell_fee_pct,
                withdraw_fee_pct=0.0,
                net_profit_pct=0.0,
                error_reason="currency_not_available_sell",
            )

    # Get networks and normalize names (with fallback support)
    if use_fallback_networks_buy:
        # Use networks from static fallback data
        buy_networks_raw = get_fallback_networks(buy_exchange, base_currency)
        buy_network_map = {normalize_network(n): n for n in buy_networks_raw}
    else:
        buy_networks_raw = set((buy_curr.get("networks") or {}).keys())
        buy_network_map = {normalize_network(n): n for n in buy_networks_raw}

    if use_fallback_networks_sell:
        # Use networks from static fallback data
        sell_networks_raw = get_fallback_networks(sell_exchange, base_currency)
        sell_network_map = {normalize_network(n): n for n in sell_networks_raw}
    else:
        sell_networks_raw = set((sell_curr.get("networks") or {}).keys())
        sell_network_map = {normalize_network(n): n for n in sell_networks_raw}

    buy_networks = set(buy_network_map.keys())
    sell_networks = set(sell_network_map.keys())

    stablecoins = ("USDT", "USDC", "DAI", "FDUSD", "TUSD")

    # FALLBACK: If no networks returned by API, infer from token mapping.
    # Critical for tokens where network label != currency code (e.g., APT -> APTOS).
    if not buy_networks and base_currency not in stablecoins:
        buy_networks = {
            resolve_network_for_token(base_currency, set()) or base_currency
        }
    if not sell_networks and base_currency not in stablecoins:
        sell_networks = {
            resolve_network_for_token(base_currency, set()) or base_currency
        }

    common_networks = buy_networks & sell_networks

    # FALLBACK: If APIs disagree or omit networks, infer from token mapping.
    if not common_networks and base_currency not in stablecoins:
        inferred = resolve_network_for_token(base_currency, set()) or base_currency
        common_networks = {inferred}
        logger.debug(
            "NETWORK FALLBACK | %s: inferred network=%s",
            symbol,
            inferred,
        )

    # DEBUG: Log network detection for non-stablecoins
    if base_currency not in stablecoins:
        buy_networks_filtered = (
            sorted(n for n in buy_networks if n is not None) if buy_networks else []
        )
        sell_networks_filtered = (
            sorted(n for n in sell_networks if n is not None) if sell_networks else []
        )
        common_networks_debug = (
            sorted(n for n in common_networks if n is not None)
            if common_networks
            else []
        )
        logger.debug(
            "NETWORK DEBUG | %s buy=%s:%s sell=%s:%s common=%s",
            symbol,
            buy_exchange,
            buy_networks_filtered or "NONE",
            sell_exchange,
            sell_networks_filtered or "NONE",
            common_networks_debug or "NONE",
        )

    # Resolve network using token-to-network mapping (debug/fallback only).
    #
    # Important: when we have per-network fee data, always select the cheapest
    # network via pick_best_network(). The token mapping exists to handle API
    # disagreement/omissions, not to override cheaper networks.
    common_networks_filtered: set[str] = {n for n in common_networks if n is not None}
    if settings.allowed_networks:
        allowed = {
            normalize_network(n) or n for n in settings.allowed_networks if n
        }
        common_networks_filtered = {
            n for n in common_networks_filtered if n in allowed
        }

    per_exchange_fees_usd = None
    if state.fee_manager and common_networks_filtered:
        per_exchange_fees_usd = state.fee_manager.get_per_exchange_fees_usd(
            exchange_id=buy_exchange,
            currency=base_currency,
            networks=[n for n in common_networks_filtered],
            current_price_usd=best_ask,
        )

    resolved_network = resolve_network_for_token(base_currency, common_networks_filtered)

    # FR2: Network selection with error reporting (cheapest-fee wins when fees exist)
    network, network_error = pick_best_network(
        common_networks_filtered,
        per_exchange_fees_usd,
        settings.trade_volume_usd,
    )
    if network_error:
        if is_fee_debug_enabled(symbol, _DEBUG_FEE_SYMBOLS):
            debug_key = f"{buy_exchange}->{sell_exchange}:{symbol}:network"
            if _FEE_DEBUG_LIMITER.should_log(debug_key):
                buy_raw = list(buy_networks_raw)
                sell_raw = list(sell_networks_raw)
                fee_keys = []
                cache_age_s: float | None = None
                if state.fee_manager and state.fee_manager.cache:
                    cache_age_s = time.time() - state.fee_manager.cache.last_updated
                    fee_keys = sorted(
                        (
                            state.fee_manager.cache.fees.get(buy_exchange, {})
                            .get(base_currency, {})
                            .keys()
                        )
                    )
                logger.warning(
                    "FEE DEBUG | %s %s->%s reason=%s base=%s buy_raw=%s sell_raw=%s buy_norm=%s sell_norm=%s common=%s resolved=%s per_ex_fees=%s fee_cache_keys=%s cache_age_s=%s",
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    network_error,
                    base_currency,
                    buy_raw[:20],
                    sell_raw[:20],
                    sorted(n for n in buy_networks if n)[:20],
                    sorted(n for n in sell_networks if n)[:20],
                    sorted(common_networks_filtered)[:20],
                    resolved_network,
                    per_exchange_fees_usd,
                    fee_keys[:30],
                    None if cache_age_s is None else round(cache_age_s, 1),
                )
        return FeeCalculationResult(
            network=None,
            withdraw_fee_base=0.0,
            buy_fee_pct=buy_fee_pct,
            sell_fee_pct=sell_fee_pct,
            withdraw_fee_pct=0.0,
            net_profit_pct=0.0,
            error_reason=network_error,
        )

    # Get withdrawal fee from dynamic API (100% coverage verified 2025-12-19)
    # NOTE: None = not found, 0.0 = free withdrawal
    withdraw_fee_base: float | None = None
    fee_confidence = "HIGH"  # Only HIGH exists now - dynamic API or reject

    if network:
        if state.fee_manager:
            # Use dynamic withdrawal fee from fee manager
            # NOTE: Returns fee in BASE CURRENCY (e.g., 0.1 LTC), NOT USD
            # Returns (None, confidence) if not found, (0.0, confidence) if free withdrawal
            withdraw_fee_base, fee_confidence = state.fee_manager.get_withdrawal_fee(
                exchange_id=buy_exchange,  # Withdraw FROM buy exchange
                currency=base_currency,
                network=network,
            )

            # FR3: Withdrawal fee validation with graceful degradation
            if withdraw_fee_base is None:
                if is_fee_debug_enabled(symbol, _DEBUG_FEE_SYMBOLS):
                    debug_key = f"{buy_exchange}->{sell_exchange}:{symbol}:withdraw_fee"
                    if _FEE_DEBUG_LIMITER.should_log(debug_key):
                        fee_keys = []
                        cache_age_s: float | None = None
                        if state.fee_manager and state.fee_manager.cache:
                            cache_age_s = (
                                time.time() - state.fee_manager.cache.last_updated
                            )
                            fee_keys = sorted(
                                (
                                    state.fee_manager.cache.fees.get(buy_exchange, {})
                                    .get(base_currency, {})
                                    .keys()
                                )
                            )
                        logger.warning(
                            "FEE DEBUG | %s %s->%s reason=no_withdrawal_fee base=%s buy=%s net=%s fee_cache_keys=%s cache_age_s=%s",
                            symbol,
                            buy_exchange,
                            sell_exchange,
                            base_currency,
                            buy_exchange,
                            network,
                            fee_keys[:30],
                            None if cache_age_s is None else round(cache_age_s, 1),
                        )
                # No dynamic fee available - reject without hot-path logging.
                return FeeCalculationResult(
                    network=None,
                    withdraw_fee_base=0.0,
                    buy_fee_pct=buy_fee_pct,
                    sell_fee_pct=sell_fee_pct,
                    withdraw_fee_pct=0.0,
                    net_profit_pct=0.0,
                    error_reason="no_withdrawal_fee",
                )

            # At this point withdraw_fee_base is not None (we returned above if it was)
            assert withdraw_fee_base is not None  # Type narrowing for pyright

            # Dynamic fee is in base currency - will be converted to USD for profit calc
            logger.debug(
                "WITHDRAW FEE | %s buy_ex=%s network=%s fee=%.4f %s",
                symbol,
                buy_exchange,
                network,
                withdraw_fee_base,
                base_currency,
            )

        else:
            return FeeCalculationResult(
                network=None,
                withdraw_fee_base=0.0,
                buy_fee_pct=buy_fee_pct,
                sell_fee_pct=sell_fee_pct,
                withdraw_fee_pct=0.0,
                net_profit_pct=0.0,
                error_reason="no_fee_manager",
            )

    else:
        return FeeCalculationResult(
            network=None,
            withdraw_fee_base=0.0,
            buy_fee_pct=buy_fee_pct,
            sell_fee_pct=sell_fee_pct,
            withdraw_fee_pct=0.0,
            net_profit_pct=0.0,
            error_reason="no_network_selected",
        )

    # Calculate net profit - convert base currency fee to USD for profit calculation
    withdraw_fee_usd = withdraw_fee_base * best_ask
    logger.debug(
        "FEE CONVERSION | %s fee_base=%.4f %s * price=%.2f = %.4f USD",
        symbol,
        withdraw_fee_base,
        base_currency,
        best_ask,
        withdraw_fee_usd,
    )

    gross_spread_pct = ((best_bid - best_ask) / best_ask) * 100.0
    net_profit_pct, trade_fees_pct, withdraw_fee_pct = compute_net_profit_pct(
        gross_spread_pct,
        buy_fee_pct,
        sell_fee_pct,
        withdraw_fee_usd,  # USD value for profit calculation
        settings.trade_volume_usd,
    )
    net_profit_funded_pct, _, _ = compute_net_profit_pct(
        gross_spread_pct,
        buy_fee_pct,
        sell_fee_pct,
        0.0,
        settings.trade_volume_usd,
    )

    # No error - return success result (store BASE currency fee for later use)
    return FeeCalculationResult(
        network=network,
        withdraw_fee_base=withdraw_fee_base,
        buy_fee_pct=buy_fee_pct,
        sell_fee_pct=sell_fee_pct,
        withdraw_fee_pct=withdraw_fee_pct,
        net_profit_pct=net_profit_pct,
        net_profit_funded_pct=net_profit_funded_pct,
        error_reason=None,
        fee_confidence=fee_confidence,
    )
