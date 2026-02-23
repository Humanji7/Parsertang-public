"""
LegExecutor - Execution logic for arbitrage cycle legs.

This module extracts leg execution logic from SimpleTrader:
- LEG1: Buy order execution with timeout
- LEG3: Sell order with market order fallback

Architecture: Composition pattern.
LegExecutor receives simulator and callbacks in constructor.
Returns results for caller to handle state transitions.
"""

import logging
from typing import TYPE_CHECKING, Optional, Callable

from parsertang.config import settings
from parsertang.trade_models import CycleState, TradeCycle
from parsertang.trader_simulation import DRY_RUN_LIQUIDITY_USAGE_PCT

if TYPE_CHECKING:
    from parsertang.trader_simulation import TradeSimulator
    from parsertang.trade_logger import TradeLogger

logger = logging.getLogger(__name__)


class LegExecutor:
    """
    Executes leg operations in arbitrage cycles.

    Uses Composition pattern - receives dependencies via constructor.
    Does NOT hold reference to SimpleTrader.
    Returns results - caller handles state transitions.
    """

    def __init__(
        self,
        simulator: Optional["TradeSimulator"],
        trade_logger: "TradeLogger",
        dry_run: bool = True,
        on_transition: Optional[Callable[[TradeCycle, CycleState], None]] = None,
    ):
        """
        Initialize LegExecutor.

        Args:
            simulator: TradeSimulator for dry-run mode (None for real mode)
            trade_logger: TradeLogger for cycle logging
            dry_run: Whether we're in dry-run mode
            on_transition: Callback for state transitions
        """
        self.simulator = simulator
        self.trade_logger = trade_logger
        self.dry_run = dry_run
        self.on_transition = on_transition

    def _transition_state(self, cycle: TradeCycle, new_state: CycleState) -> None:
        """
        Trigger state transition via callback.

        Args:
            cycle: TradeCycle to transition
            new_state: New CycleState
        """
        if self.on_transition:
            self.on_transition(cycle, new_state)
        else:
            # Fallback: direct transition (less ideal but works)
            old_state = cycle.state
            cycle.state = new_state
            logger.debug(
                "CYCLE STATE | %s | %s → %s",
                cycle.cycle_id,
                old_state.value,
                new_state.value,
            )

    async def execute_leg1(self, cycle: TradeCycle) -> bool:
        """
        Execute LEG1: Buy order with timeout.

        In dry-run mode:
        - Simulates limit order placement
        - Calculates position size based on MAX_POSITION_SIZE_USD
        - Simulates order fill (instant or timeout after 5s)
        - Tracks position and fees

        Args:
            cycle: TradeCycle to execute

        Returns:
            True if successful (order filled), False if timeout/failed
        """
        # Transition to LEG1 state
        self._transition_state(cycle, CycleState.LEG1)

        assert cycle.opportunity is not None, "LEG1: opportunity must be set"
        opp = cycle.opportunity

        # Calculate position size
        max_position_usd = settings.max_position_size_usd
        buy_price = opp.buy_price
        base_amount = max_position_usd / buy_price

        # Check liquidity constraints
        max_amount_by_liquidity = opp.ask_liq_usd / buy_price
        actual_amount = min(
            base_amount, max_amount_by_liquidity * DRY_RUN_LIQUIDITY_USAGE_PCT
        )
        position_value = actual_amount * buy_price

        cycle.log_event(
            CycleState.LEG1,
            f"LEG1 started: {opp.buy_exchange} buy {cycle.base_currency}",
            {
                "exchange": opp.buy_exchange,
                "symbol": opp.symbol,
                "price": buy_price,
                "amount": actual_amount,
                "value_usd": position_value,
            },
        )

        logger.info(
            "CYCLE LEG1 | %s | Buy order: %s @ %.8f x %.8f = %.2f USD on %s",
            cycle.cycle_id,
            opp.symbol,
            buy_price,
            actual_amount,
            position_value,
            opp.buy_exchange,
        )

        # Simulate order placement (dry-run or real)
        if self.dry_run and self.simulator:
            result = await self.simulator.simulate_buy_order(
                exchange=opp.buy_exchange,
                symbol=opp.symbol,
                price=buy_price,
                amount=actual_amount,
                cycle_id=cycle.cycle_id,
            )
            success = result.success
            if result.order_info:
                cycle.leg1_order = result.order_info
            # Handle suggested state transition
            if not success and result.suggested_state:
                self._transition_state(cycle, result.suggested_state)
        else:
            # TODO Phase R5: Real order execution
            logger.warning(
                "CYCLE LEG1 | %s | Real trading not implemented yet", cycle.cycle_id
            )
            success = False

        if success:
            # Update position tracking
            cycle.position_amount = actual_amount
            cycle.position_value_usd = position_value

            # Calculate buy fees using actual buy exchange taker fee
            buy_fee_pct = opp.buy_taker_fee_pct
            buy_fee_usd = position_value * (buy_fee_pct / 100.0)
            cycle.total_fees_usd += buy_fee_usd

            cycle.log_event(
                CycleState.LEG1,
                f"LEG1 completed: bought {actual_amount:.8f} {cycle.base_currency}",
                {
                    "filled": actual_amount,
                    "average_price": buy_price,
                    "fee_usd": buy_fee_usd,
                },
            )

            logger.info(
                "CYCLE LEG1 | %s | Buy order FILLED: %.8f %s @ %.8f (fee: %.4f USD)",
                cycle.cycle_id,
                actual_amount,
                cycle.base_currency,
                buy_price,
                buy_fee_usd,
            )
        else:
            cycle.log_event(
                CycleState.LEG1_TIMEOUT,
                "LEG1 timeout: buy order not filled within timeout",
                {"timeout_seconds": settings.leg1_timeout_seconds},
            )

            logger.warning(
                "CYCLE LEG1 | %s | Buy order TIMEOUT after %ds",
                cycle.cycle_id,
                settings.leg1_timeout_seconds,
            )

        # Log state update
        self.trade_logger.log_cycle_update(
            cycle, "LEG1_COMPLETED" if success else "LEG1_TIMEOUT"
        )

        return success

    async def execute_leg3(self, cycle: TradeCycle) -> bool:
        """
        Execute LEG3: Sell order with market order fallback.

        In dry-run mode:
        - Simulates limit order placement
        - Timeout handling (LEG3_TIMEOUT_SECONDS)
        - Falls back to market order if timeout
        - Calculates realized profit

        Args:
            cycle: TradeCycle to execute

        Returns:
            True if successful (order filled), False if failed
        """
        # Transition to LEG3 state
        self._transition_state(cycle, CycleState.LEG3)

        assert cycle.opportunity is not None, "LEG3: opportunity must be set"
        assert cycle.position_amount > 0, "LEG3: no position to sell"
        opp = cycle.opportunity

        # Sell parameters
        sell_price = opp.sell_price
        sell_amount = cycle.position_amount
        sell_value = sell_amount * sell_price

        cycle.log_event(
            CycleState.LEG3,
            f"LEG3 started: {opp.sell_exchange} sell {cycle.base_currency}",
            {
                "exchange": opp.sell_exchange,
                "symbol": opp.symbol,
                "price": sell_price,
                "amount": sell_amount,
                "value_usd": sell_value,
            },
        )

        logger.info(
            "CYCLE LEG3 | %s | Sell order: %s @ %.8f x %.8f = %.2f USD on %s",
            cycle.cycle_id,
            opp.symbol,
            sell_price,
            sell_amount,
            sell_value,
            opp.sell_exchange,
        )

        # Try limit order first
        if self.dry_run and self.simulator:
            result = await self.simulator.simulate_sell_order(
                exchange=opp.sell_exchange,
                symbol=opp.symbol,
                price=sell_price,
                amount=sell_amount,
                order_type="limit",
                cycle_id=cycle.cycle_id,
            )
            success = result.success
            filled_price = result.filled_price
            if result.order_info:
                cycle.leg3_order = result.order_info
        else:
            # TODO Phase R5: Real order execution
            logger.warning(
                "CYCLE LEG3 | %s | Real trading not implemented yet", cycle.cycle_id
            )
            success = False
            filled_price = 0.0

        # If limit order timeout, try market order
        if not success:
            logger.info(
                "CYCLE LEG3 | %s | Limit order timeout, falling back to market order",
                cycle.cycle_id,
            )

            self._transition_state(cycle, CycleState.LEG3_MARKET)
            cycle.log_event(
                CycleState.LEG3_MARKET,
                "LEG3 limit timeout, trying market order",
                {"timeout_seconds": settings.leg3_timeout_seconds},
            )

            if self.dry_run and self.simulator:
                result = await self.simulator.simulate_sell_order(
                    exchange=opp.sell_exchange,
                    symbol=opp.symbol,
                    price=sell_price,
                    amount=sell_amount,
                    order_type="market",
                    cycle_id=cycle.cycle_id,
                )
                success = result.success
                filled_price = result.filled_price
                if result.order_info:
                    cycle.leg3_order = result.order_info
            else:
                # TODO Phase R5: Real market order execution
                success = False
                filled_price = 0.0

        if success:
            # Calculate realized profit
            sell_value_actual = sell_amount * filled_price

            # Calculate sell fee using actual sell exchange taker fee
            sell_fee_pct = opp.sell_taker_fee_pct
            sell_fee_usd = sell_value_actual * (sell_fee_pct / 100.0)
            cycle.total_fees_usd += sell_fee_usd

            # Final profit calculation
            cycle.realized_profit_usd = (
                sell_value_actual - cycle.position_value_usd - cycle.total_fees_usd
            )
            if cycle.position_value_usd > 0:
                cycle.realized_profit_pct = (
                    cycle.realized_profit_usd / cycle.position_value_usd
                ) * 100.0

            cycle.log_event(
                CycleState.LEG3,
                f"LEG3 completed: sold {sell_amount:.8f} {cycle.base_currency}",
                {
                    "filled": sell_amount,
                    "average_price": filled_price,
                    "value_usd": sell_value_actual,
                    "fee_usd": sell_fee_usd,
                    "profit_usd": cycle.realized_profit_usd,
                    "profit_pct": cycle.realized_profit_pct,
                },
            )

            logger.info(
                "CYCLE LEG3 | %s | Sell order FILLED: %.8f %s @ %.8f = %.2f USD (fee: %.4f USD)",
                cycle.cycle_id,
                sell_amount,
                cycle.base_currency,
                filled_price,
                sell_value_actual,
                sell_fee_usd,
            )
            logger.info(
                "CYCLE LEG3 | %s | PROFIT: %.2f%% (%.2f USD) | Total fees: %.4f USD",
                cycle.cycle_id,
                cycle.realized_profit_pct,
                cycle.realized_profit_usd,
                cycle.total_fees_usd,
            )
        else:
            cycle.log_event(
                CycleState.LEG3,
                "LEG3 failed: sell order failed (both limit and market)",
                {},
            )

            logger.warning(
                "CYCLE LEG3 | %s | Sell order FAILED (tried both limit and market)",
                cycle.cycle_id,
            )

        # Log state update
        self.trade_logger.log_cycle_update(
            cycle, "LEG3_COMPLETED" if success else "LEG3_FAILED"
        )

        return success
