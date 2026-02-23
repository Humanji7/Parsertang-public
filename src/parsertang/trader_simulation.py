"""
TradeSimulator - Stateless dry-run simulation for 3-leg arbitrage cycles.

This module provides simulation methods for:
- LEG1: Buy order simulation with timeout
- LEG2: Withdrawal simulation with network delays
- LEG3: Sell order simulation (limit/market)

Architecture: Stateless design - no reference to SimpleTrader.
Returns SimulationResult instead of calling _transition_state.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from parsertang.config import settings
from parsertang.trade_models import CycleState, OrderInfo, WithdrawalInfo


logger = logging.getLogger(__name__)


# =============================================================================
# DRY-RUN SIMULATION PARAMETERS
# =============================================================================
# These constants control the behavior of dry-run mode simulations.
# They approximate real market conditions with realistic success rates and delays.

# Liquidity usage
DRY_RUN_LIQUIDITY_USAGE_PCT = 0.9  # Use 90% of available liquidity for safety margin

# LEG1 (Buy order) simulation
DRY_RUN_LEG1_SUCCESS_RATE = 0.9  # 90% chance limit order fills within timeout
DRY_RUN_LEG1_DELAY_MIN = 0.1  # Minimum fill delay in seconds
DRY_RUN_LEG1_DELAY_MAX = 0.5  # Maximum fill delay in seconds

# LEG2 (Withdrawal) simulation
DRY_RUN_WITHDRAWAL_SUCCESS_RATE = 0.95  # 95% success rate (network issues rare)

# LEG3 (Sell order) simulation
DRY_RUN_LEG3_LIMIT_SUCCESS_RATE = 0.7  # 70% chance limit order fills
DRY_RUN_LEG3_MARKET_SUCCESS_RATE = 0.98  # 98% chance market order succeeds
DRY_RUN_LEG3_SLIPPAGE_MIN_PCT = -0.15  # Minimum slippage (negative = worse price)
DRY_RUN_LEG3_SLIPPAGE_MAX_PCT = -0.05  # Maximum slippage

# Fallback withdrawal fee (when API unavailable)
DRY_RUN_FALLBACK_WITHDRAWAL_FEE = 0.0001  # 0.01% of withdrawal amount
DRY_RUN_FALLBACK_WITHDRAWAL_MIN = 0.001  # Only apply if amount > 0.001


@dataclass
class SimulationResult:
    """Result of a simulation operation.

    Attributes:
        success: Whether the operation succeeded
        order_info: Order details (for buy/sell operations)
        withdrawal_info: Withdrawal details (for withdrawal operations)
        suggested_state: Suggested state transition (caller decides if to apply)
        filled_price: Actual filled price (for sell orders with slippage)
        error: Error message if failed
    """

    success: bool
    order_info: Optional[OrderInfo] = None
    withdrawal_info: Optional[WithdrawalInfo] = None
    suggested_state: Optional[CycleState] = None
    filled_price: float = 0.0
    error: Optional[str] = None


class TradeSimulator:
    """
    Stateless simulator for dry-run mode.

    Does NOT hold reference to SimpleTrader or TradeCycle.
    Returns SimulationResult - caller handles state transitions.
    """

    def __init__(self):
        """Initialize TradeSimulator."""
        logger.debug("TradeSimulator initialized")

    async def simulate_buy_order(
        self,
        exchange: str,
        symbol: str,
        price: float,
        amount: float,
        cycle_id: str,
    ) -> SimulationResult:
        """
        Simulate buy order execution in dry-run mode.

        Simulates:
        - Limit order placement
        - Timeout behavior (LEG1_TIMEOUT_SECONDS)
        - Instant fill (90% probability) or timeout (10% probability)

        Args:
            exchange: Exchange to buy on
            symbol: Trading symbol
            price: Limit order price
            amount: Amount to buy
            cycle_id: Cycle ID for logging

        Returns:
            SimulationResult with order_info and suggested_state
        """
        # Create order info
        order_id = f"dry_{uuid.uuid4().hex[:12]}"
        order_info = OrderInfo(
            order_id=order_id,
            order_type="limit",
            side="buy",
            price=price,
            amount=amount,
            filled=0.0,
            average_price=0.0,
            status="open",
        )

        logger.debug(
            "CYCLE LEG1 | %s | Dry-run: placed limit buy order %s",
            cycle_id,
            order_id,
        )

        # Simulate order execution with timeout
        timeout = settings.leg1_timeout_seconds
        start_time = datetime.utcnow()

        # In dry-run, simulate instant fill with high probability (90%)
        # or timeout (10%) to test both paths
        will_fill = random.random() < DRY_RUN_LEG1_SUCCESS_RATE

        if will_fill:
            # Simulate small delay (0.1-0.5s)
            delay = random.uniform(DRY_RUN_LEG1_DELAY_MIN, DRY_RUN_LEG1_DELAY_MAX)
            await asyncio.sleep(delay)

            # Order filled
            order_info.status = "closed"
            order_info.filled = amount
            order_info.average_price = price
            order_info.timestamp = datetime.utcnow()

            logger.debug(
                "CYCLE LEG1 | %s | Dry-run: order %s filled after %.2fs",
                cycle_id,
                order_id,
                delay,
            )

            return SimulationResult(
                success=True,
                order_info=order_info,
                suggested_state=None,  # No state change needed on success
            )
        else:
            # Simulate timeout
            await asyncio.sleep(timeout)

            elapsed = (datetime.utcnow() - start_time).total_seconds()

            # Order timed out
            order_info.status = "timeout"
            order_info.filled = 0.0
            order_info.timestamp = datetime.utcnow()

            logger.debug(
                "CYCLE LEG1 | %s | Dry-run: order %s timeout after %.2fs",
                cycle_id,
                order_id,
                elapsed,
            )

            # Return suggested state - caller decides transition
            return SimulationResult(
                success=False,
                order_info=order_info,
                suggested_state=CycleState.LEG1_TIMEOUT,
                error=f"Order timeout after {elapsed:.2f}s",
            )

    async def simulate_withdrawal(
        self,
        from_exchange: str,
        to_exchange: str,
        currency: str,
        amount: float,
        network: str,
        fee: float,
        cycle_id: str,
    ) -> SimulationResult:
        """
        Simulate withdrawal execution in dry-run mode.

        Simulates:
        - Withdrawal initiation
        - Network confirmation delay (1-3s for fast networks, 5-10s for slow)
        - Success/failure (95% success rate)

        Args:
            from_exchange: Source exchange
            to_exchange: Destination exchange
            currency: Currency to withdraw
            amount: Amount to withdraw
            network: Network to use (e.g., TRC20, BEP20)
            fee: Withdrawal fee
            cycle_id: Cycle ID for logging

        Returns:
            SimulationResult with withdrawal_info
        """
        # Create withdrawal info
        withdrawal_id = f"dry_{uuid.uuid4().hex[:12]}"
        withdrawal_info = WithdrawalInfo(
            withdrawal_id=withdrawal_id,
            currency=currency,
            network=network,
            amount=amount,
            fee=fee,
            address="dry_run_address",
            status="pending",
        )

        logger.debug(
            "CYCLE LEG2 | %s | Dry-run: initiated withdrawal %s",
            cycle_id,
            withdrawal_id,
        )

        # Simulate network confirmation delay
        # Fast networks (TRC20, BEP20, SOL): 1-3s
        # Slow networks (BTC, ETH): 5-10s
        fast_networks = ["TRC20", "BEP20", "SOL", "SOLANA", "BSC", "TRON"]
        is_fast = any(net.upper() in network.upper() for net in fast_networks)

        if is_fast:
            delay = random.uniform(1.0, 3.0)
        else:
            delay = random.uniform(5.0, 10.0)

        await asyncio.sleep(delay)

        # Simulate success/failure (95% success rate)
        will_succeed = random.random() < DRY_RUN_WITHDRAWAL_SUCCESS_RATE

        if will_succeed:
            # Withdrawal successful
            withdrawal_info.status = "completed"
            withdrawal_info.timestamp = datetime.utcnow()

            logger.debug(
                "CYCLE LEG2 | %s | Dry-run: withdrawal %s completed after %.2fs",
                cycle_id,
                withdrawal_id,
                delay,
            )

            return SimulationResult(
                success=True,
                withdrawal_info=withdrawal_info,
            )
        else:
            # Withdrawal failed
            withdrawal_info.status = "failed"
            withdrawal_info.timestamp = datetime.utcnow()

            logger.debug(
                "CYCLE LEG2 | %s | Dry-run: withdrawal %s failed after %.2fs",
                cycle_id,
                withdrawal_id,
                delay,
            )

            return SimulationResult(
                success=False,
                withdrawal_info=withdrawal_info,
                error=f"Withdrawal failed after {delay:.2f}s",
            )

    async def simulate_sell_order(
        self,
        exchange: str,
        symbol: str,
        price: float,
        amount: float,
        order_type: str,
        cycle_id: str,
    ) -> SimulationResult:
        """
        Simulate sell order execution in dry-run mode.

        Simulates:
        - Limit order: timeout behavior (LEG3_TIMEOUT_SECONDS), 70% fill rate
        - Market order: instant fill with slippage (-0.05% to -0.15%)

        Args:
            exchange: Exchange to sell on
            symbol: Trading symbol
            price: Order price (for limit orders)
            amount: Amount to sell
            order_type: "limit" or "market"
            cycle_id: Cycle ID for logging

        Returns:
            SimulationResult with order_info and filled_price
        """
        # Create order info
        order_id = f"dry_{uuid.uuid4().hex[:12]}"
        order_info = OrderInfo(
            order_id=order_id,
            order_type=order_type,
            side="sell",
            price=price,
            amount=amount,
            filled=0.0,
            average_price=0.0,
            status="open",
        )

        logger.debug(
            "CYCLE LEG3 | %s | Dry-run: placed %s sell order %s",
            cycle_id,
            order_type,
            order_id,
        )

        if order_type == "limit":
            # Simulate limit order with timeout
            timeout = settings.leg3_timeout_seconds

            # 70% chance of fill before timeout
            will_fill = random.random() < DRY_RUN_LEG3_LIMIT_SUCCESS_RATE

            if will_fill:
                # Fill within timeout (0.5-8s)
                delay = random.uniform(0.5, min(8.0, timeout - 0.1))
                await asyncio.sleep(delay)

                # Order filled at limit price
                order_info.status = "closed"
                order_info.filled = amount
                order_info.average_price = price
                order_info.timestamp = datetime.utcnow()

                logger.debug(
                    "CYCLE LEG3 | %s | Dry-run: limit order %s filled after %.2fs",
                    cycle_id,
                    order_id,
                    delay,
                )

                return SimulationResult(
                    success=True,
                    order_info=order_info,
                    filled_price=price,
                )
            else:
                # Timeout
                await asyncio.sleep(timeout)

                order_info.status = "timeout"
                order_info.filled = 0.0
                order_info.timestamp = datetime.utcnow()

                logger.debug(
                    "CYCLE LEG3 | %s | Dry-run: limit order %s timeout after %ds",
                    cycle_id,
                    order_id,
                    timeout,
                )

                return SimulationResult(
                    success=False,
                    order_info=order_info,
                    error=f"Limit order timeout after {timeout}s",
                )

        else:  # market order
            # Market orders fill instantly with slippage
            delay = random.uniform(0.1, 0.3)
            await asyncio.sleep(delay)

            # Simulate slippage: -0.05% to -0.15% (worse price for seller)
            slippage_pct = random.uniform(
                DRY_RUN_LEG3_SLIPPAGE_MIN_PCT, DRY_RUN_LEG3_SLIPPAGE_MAX_PCT
            )
            filled_price = price * (1.0 + slippage_pct / 100.0)

            # 98% success rate for market orders
            will_succeed = random.random() < DRY_RUN_LEG3_MARKET_SUCCESS_RATE

            if will_succeed:
                order_info.status = "closed"
                order_info.filled = amount
                order_info.average_price = filled_price
                order_info.timestamp = datetime.utcnow()

                logger.debug(
                    "CYCLE LEG3 | %s | Dry-run: market order %s filled @ %.8f (slippage: %.2f%%)",
                    cycle_id,
                    order_id,
                    filled_price,
                    slippage_pct,
                )

                return SimulationResult(
                    success=True,
                    order_info=order_info,
                    filled_price=filled_price,
                )
            else:
                # Market order failed (rare)
                order_info.status = "failed"
                order_info.filled = 0.0
                order_info.timestamp = datetime.utcnow()

                logger.debug(
                    "CYCLE LEG3 | %s | Dry-run: market order %s failed",
                    cycle_id,
                    order_id,
                )

                return SimulationResult(
                    success=False,
                    order_info=order_info,
                    error="Market order failed",
                )
