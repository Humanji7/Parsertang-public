"""
SimpleTrader - 3-leg arbitrage cycle executor (Phase R2+)

Manages the complete lifecycle of arbitrage trading cycles:
- LEG1: Auto buy (limit order with timeout)
- LEG2: Semi-auto withdrawal (requires confirmation in R3+)
- LEG3: Auto sell (limit order → market order fallback)

Dry-run mode (R2): Simulates all operations without real orders.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from parsertang.arbitrage import Opportunity
from parsertang.config import settings
from parsertang.trade_logger import TradeLogger
from parsertang.trade_models import CycleState, TradeCycle
from parsertang.trader_simulation import (
    TradeSimulator,
    DRY_RUN_FALLBACK_WITHDRAWAL_FEE,
    DRY_RUN_FALLBACK_WITHDRAWAL_MIN,
)
from parsertang.trader_telegram import TelegramLeg2Handler
from parsertang.trader_legs import LegExecutor

if TYPE_CHECKING:
    from parsertang.alerts import AlertService


logger = logging.getLogger(__name__)


# DRY_RUN constants are now imported from trader_simulation.py


class SimpleTrader:
    """
    Simple 3-leg arbitrage cycle executor.

    Manages cycle state machine and executes trades in either
    dry-run (simulation) or real mode.

    State Flow:
        SCANNING → LEG1 → LEG2_WAIT → LEG3 → COMPLETE
                    ↓         ↓          ↓
                  FAILED    FAILED    FAILED
    """

    def __init__(
        self,
        dry_run: bool = True,
        max_concurrent_cycles: int = 1,
        trade_logger: Optional[TradeLogger] = None,
        alert_service: Optional[AlertService] = None,
    ):
        """
        Initialize SimpleTrader.

        Args:
            dry_run: If True, simulate trades without real orders
            max_concurrent_cycles: Maximum number of concurrent cycles
            trade_logger: Optional TradeLogger instance (creates default if None)
            alert_service: Optional AlertService for Telegram notifications
        """
        self.dry_run = dry_run
        self.max_concurrent_cycles = max_concurrent_cycles

        # Trade logger
        self.trade_logger = trade_logger or TradeLogger.get_instance()

        # Alert service for Telegram notifications
        self.alert_service = alert_service

        # Simulation engine (stateless)
        self.simulator = TradeSimulator() if dry_run else None

        # Telegram handler for LEG2 confirmation (Composition pattern)
        self.telegram_handler = TelegramLeg2Handler(alert_service=alert_service)

        # Leg executor for LEG1/LEG3 (Composition pattern)
        self.leg_executor = LegExecutor(
            simulator=self.simulator,
            trade_logger=self.trade_logger,
            dry_run=dry_run,
            on_transition=self._transition_state,
        )

        # Active cycles tracking
        self.active_cycles: dict[str, TradeCycle] = {}

        # Statistics
        self.total_cycles_started = 0
        self.total_cycles_completed = 0
        self.total_cycles_failed = 0

        # Background task tracking (prevents lost exceptions)
        self._background_tasks: set[asyncio.Task] = set()

        logger.info(
            "SimpleTrader initialized: dry_run=%s, max_concurrent=%d",
            dry_run,
            max_concurrent_cycles,
        )

    def _create_tracked_task(
        self,
        coro,
        name: str,  # noqa: ANN001 (Coroutine type too complex)
    ) -> asyncio.Task:
        """
        Create a background task with automatic exception logging.

        Prevents fire-and-forget tasks from silently losing exceptions.

        Args:
            coro: Coroutine to run
            name: Task name for logging

        Returns:
            Created task (also tracked in self._background_tasks)
        """
        task = asyncio.create_task(coro)

        def _on_done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                logger.error("Background task '%s' failed: %s", name, exc, exc_info=exc)

        task.add_done_callback(_on_done)
        self._background_tasks.add(task)
        return task

    def can_start_new_cycle(self) -> bool:
        """
        Check if a new cycle can be started.

        Returns:
            True if under max concurrent cycles limit
        """
        return len(self.active_cycles) < self.max_concurrent_cycles

    def evaluate_opportunity(self, opportunity: Opportunity) -> bool:
        """
        Evaluate if an opportunity should trigger a new cycle.

        Decision criteria:
        1. Must meet minimum net profit threshold
        2. Must not exceed max concurrent cycles
        3. Must have sufficient liquidity
        4. Dry-run mode: only if TRADING_ENABLED=false

        Args:
            opportunity: Arbitrage opportunity to evaluate

        Returns:
            True if cycle should be started, False otherwise
        """
        # Check if trading is enabled (for dry-run validation)
        if not self.dry_run and not settings.trading_enabled:
            logger.debug("Trading disabled, skipping opportunity")
            return False

        # Check concurrent cycles limit
        if not self.can_start_new_cycle():
            logger.debug(
                "Max concurrent cycles reached (%d/%d), skipping opportunity",
                len(self.active_cycles),
                self.max_concurrent_cycles,
            )
            return False

        # Check profit threshold
        if opportunity.net_profit_pct < settings.min_net_profit:
            logger.debug(
                "Net profit %.2f%% below threshold %.2f%%, skipping",
                opportunity.net_profit_pct,
                settings.min_net_profit,
            )
            return False

        # Check liquidity
        min_liquidity = settings.liquidity_usd_threshold
        if (
            opportunity.ask_liq_usd < min_liquidity
            or opportunity.bid_liq_usd < min_liquidity
        ):
            logger.debug(
                "Insufficient liquidity (ask=%.2f, bid=%.2f, min=%.2f), skipping",
                opportunity.ask_liq_usd,
                opportunity.bid_liq_usd,
                min_liquidity,
            )
            return False

        # All checks passed
        logger.info(
            "Opportunity ACCEPTED: %s %s→%s net_profit=%.2f%%",
            opportunity.symbol,
            opportunity.buy_exchange,
            opportunity.sell_exchange,
            opportunity.net_profit_pct,
        )
        return True

    async def start_cycle(self, opportunity: Opportunity) -> Optional[str]:
        """
        Start a new arbitrage cycle.

        Creates a TradeCycle, logs initial state, and begins execution.

        Args:
            opportunity: Arbitrage opportunity to execute

        Returns:
            Cycle ID if started, None if failed
        """
        if not self.evaluate_opportunity(opportunity):
            return None

        # Create cycle
        cycle = TradeCycle(opportunity=opportunity)
        cycle.started_at = datetime.utcnow()

        # Extract currency info
        if "/" in opportunity.symbol:
            parts = opportunity.symbol.split("/")
            cycle.base_currency = parts[0]
            cycle.quote_currency = parts[1]

        # Log initial event
        cycle.log_event(
            CycleState.SCANNING,
            f"Cycle started: {opportunity.symbol} {opportunity.buy_exchange}→{opportunity.sell_exchange}",
            {
                "net_profit_pct": opportunity.net_profit_pct,
                "network": opportunity.network,
            },
        )

        # Add to active cycles
        self.active_cycles[cycle.cycle_id] = cycle
        self.total_cycles_started += 1

        logger.info(
            "CYCLE START | %s | %s %s→%s | net_profit=%.2f%% | network=%s",
            cycle.cycle_id,
            opportunity.symbol,
            opportunity.buy_exchange,
            opportunity.sell_exchange,
            opportunity.net_profit_pct,
            opportunity.network or "N/A",
        )

        # Log initial state
        self.trade_logger.log_cycle_update(cycle, "CYCLE_STARTED")

        # Start execution in background (tracked to prevent lost exceptions)
        self._create_tracked_task(
            self._execute_cycle(cycle), f"execute_cycle_{cycle.cycle_id}"
        )

        return cycle.cycle_id

    async def _execute_cycle(self, cycle: TradeCycle) -> None:
        """
        Execute complete 3-leg cycle.

        This is the main orchestration method that runs:
        LEG1 → LEG2 → LEG3 → COMPLETE (or FAILED at any stage)

        Args:
            cycle: TradeCycle to execute
        """
        try:
            # LEG1: Buy
            success = await self._execute_leg1(cycle)
            if not success:
                await self._fail_cycle(cycle, "LEG1 failed")
                return

            # LEG2: Withdrawal
            success = await self._execute_leg2(cycle)
            if not success:
                await self._fail_cycle(cycle, "LEG2 failed")
                return

            # LEG3: Sell
            success = await self._execute_leg3(cycle)
            if not success:
                await self._fail_cycle(cycle, "LEG3 failed")
                return

            # Complete
            await self._complete_cycle(cycle)

        except Exception as e:
            logger.error(
                "CYCLE ERROR | %s | Unexpected error: %s",
                cycle.cycle_id,
                e,
                exc_info=True,
            )
            await self._fail_cycle(cycle, f"Unexpected error: {e}")

    async def _execute_leg1(self, cycle: TradeCycle) -> bool:
        """
        Execute LEG1: Buy order with timeout.

        Delegates to LegExecutor.

        Args:
            cycle: TradeCycle to execute

        Returns:
            True if successful (order filled), False if timeout/failed
        """
        return await self.leg_executor.execute_leg1(cycle)

    # NOTE: _simulate_buy_order, _simulate_withdrawal, _simulate_sell_order
    # have been moved to trader_simulation.py (TradeSimulator class)

    def _calculate_leg2_withdrawal_fee(
        self,
        cycle: TradeCycle,
        opp: "Opportunity",
        withdrawal_amount: float,
    ) -> float:
        """
        Calculate withdrawal fee in base currency coins.

        Args:
            cycle: Current trade cycle
            opp: Arbitrage opportunity with fee metadata
            withdrawal_amount: Amount to withdraw in base currency

        Returns:
            Withdrawal fee in base currency coins
        """
        # Validate buy_price before fee conversion (Issue 2.1 - P2)
        if opp.buy_price <= 0:
            logger.error(
                f"CYCLE LEG2 | {cycle.cycle_id} | Invalid buy_price in opportunity: {opp.buy_price}"
            )
            # Schedule failure asynchronously since we're in sync context (tracked)
            self._create_tracked_task(
                self._fail_cycle(cycle, f"Invalid buy price: {opp.buy_price}"),
                f"fail_cycle_{cycle.cycle_id}",
            )
            return 0.0

        # Fee is already in base currency (e.g., 0.0069 LTC)
        if hasattr(opp, "withdrawal_fee_base") and opp.withdrawal_fee_base > 0:
            return opp.withdrawal_fee_base

        # Fallback: estimate from withdraw_fee_pct (legacy)
        if hasattr(opp, "withdraw_fee_pct") and opp.withdraw_fee_pct:
            return withdrawal_amount * (opp.withdraw_fee_pct / 100.0)

        # Default: 0.0001 for common tokens
        return (
            DRY_RUN_FALLBACK_WITHDRAWAL_FEE
            if withdrawal_amount > DRY_RUN_FALLBACK_WITHDRAWAL_MIN
            else 0.0
        )

    async def _wait_for_leg2_confirmation_or_autoconfirm(
        self, cycle: TradeCycle
    ) -> bool:
        """
        Wait for LEG2 withdrawal confirmation or auto-confirm based on phase.

        Phase R2: Auto-confirms (backward compatible)
        Phase R3+: Waits for Telegram /confirm command

        Args:
            cycle: Current trade cycle

        Returns:
            True if confirmed, False if cancelled or timeout
        """
        # Send Telegram notification (Phase R3)
        await self._send_leg2_notification(cycle)

        # Phase R2: Auto-confirm (backward compatible)
        if settings.current_phase == "R2" or not self.alert_service:
            logger.info(
                "CYCLE LEG2 | %s | Phase R2 mode: auto-confirming withdrawal",
                cycle.cycle_id,
            )
            return True

        # Phase R3: Wait for Telegram confirmation
        logger.info(
            "CYCLE LEG2 | %s | Phase R3 mode: waiting for Telegram confirmation",
            cycle.cycle_id,
        )
        return await self._wait_for_leg2_confirmation(cycle)

    async def _perform_withdrawal(
        self,
        cycle: TradeCycle,
        opp: "Opportunity",
        withdrawal_amount: float,
        network: str,
        withdrawal_fee: float,
    ) -> bool:
        """
        Perform withdrawal execution (dry-run or real).

        Args:
            cycle: Current trade cycle
            opp: Arbitrage opportunity
            withdrawal_amount: Amount to withdraw
            network: Network to use for withdrawal
            withdrawal_fee: Calculated withdrawal fee

        Returns:
            True if successful, False if failed
        """
        if self.dry_run and self.simulator:
            result = await self.simulator.simulate_withdrawal(
                from_exchange=opp.buy_exchange,
                to_exchange=opp.sell_exchange,
                currency=cycle.base_currency or "",
                amount=withdrawal_amount,
                network=network,
                fee=withdrawal_fee,
                cycle_id=cycle.cycle_id,
            )
            if result.withdrawal_info:
                cycle.leg2_withdrawal = result.withdrawal_info
            return result.success

        # TODO Phase R5: Real withdrawal execution
        logger.warning(
            "CYCLE LEG2 | %s | Real withdrawal not implemented yet", cycle.cycle_id
        )
        return False

    def _finalize_leg2_execution(
        self,
        cycle: TradeCycle,
        opp: "Opportunity",
        withdrawal_amount: float,
        withdrawal_fee: float,
        success: bool,
    ) -> None:
        """
        Finalize LEG2 execution by updating cycle state and logging.

        Args:
            cycle: Current trade cycle
            opp: Arbitrage opportunity
            withdrawal_amount: Amount withdrawn
            withdrawal_fee: Withdrawal fee charged
            success: Whether withdrawal succeeded
        """
        if success:
            # Update position tracking (amount reduced by withdrawal fee)
            cycle.position_amount = withdrawal_amount - withdrawal_fee

            # Track total fees - convert base currency to USD
            withdrawal_fee_usd = opp.withdrawal_fee_base * opp.buy_price
            cycle.total_fees_usd += withdrawal_fee_usd

            cycle.log_event(
                CycleState.LEG2,
                f"LEG2 completed: withdrawn {withdrawal_amount:.8f} {cycle.base_currency}",
                {
                    "net_amount": cycle.position_amount,
                    "fee_base": withdrawal_fee,
                    "fee_usd": withdrawal_fee_usd,
                },
            )

            logger.info(
                "CYCLE LEG2 | %s | Withdrawal COMPLETED: %.8f %s received (fee: %.8f)",
                cycle.cycle_id,
                cycle.position_amount,
                cycle.base_currency,
                withdrawal_fee,
            )
        else:
            cycle.log_event(
                CycleState.LEG2,
                "LEG2 failed: withdrawal error",
                {},
            )

            logger.warning(
                "CYCLE LEG2 | %s | Withdrawal FAILED",
                cycle.cycle_id,
            )

        # Log state update
        self.trade_logger.log_cycle_update(
            cycle, "LEG2_COMPLETED" if success else "LEG2_FAILED"
        )

    async def _execute_leg2(self, cycle: TradeCycle) -> bool:
        """
        Execute LEG2: Withdrawal.

        Orchestrates the complete LEG2 flow:
        1. Calculate withdrawal fee
        2. Log initiation
        3. Wait for confirmation (Phase R3) or auto-confirm (Phase R2)
        4. Perform withdrawal
        5. Finalize state updates

        Args:
            cycle: TradeCycle to execute

        Returns:
            True if successful, False if failed
        """
        # Transition to LEG2_WAIT state
        self._transition_state(cycle, CycleState.LEG2_WAIT)

        assert cycle.opportunity is not None, "LEG2: opportunity must be set"
        assert cycle.position_amount > 0, "LEG2: no position to withdraw"
        opp = cycle.opportunity

        # Get withdrawal details
        network = opp.network or "UNKNOWN"
        withdrawal_amount = cycle.position_amount

        # Calculate withdrawal fee
        withdrawal_fee = self._calculate_leg2_withdrawal_fee(
            cycle, opp, withdrawal_amount
        )

        # Log initiation
        cycle.log_event(
            CycleState.LEG2_WAIT,
            f"LEG2 initiated: withdraw {cycle.base_currency} from {opp.buy_exchange}",
            {
                "from_exchange": opp.buy_exchange,
                "to_exchange": opp.sell_exchange,
                "currency": cycle.base_currency,
                "amount": withdrawal_amount,
                "network": network,
                "fee": withdrawal_fee,
            },
        )

        logger.info(
            "CYCLE LEG2 | %s | Withdrawal: %.8f %s via %s (fee: %.8f)",
            cycle.cycle_id,
            withdrawal_amount,
            cycle.base_currency,
            network,
            withdrawal_fee,
        )

        # Wait for confirmation or auto-confirm
        confirmed = await self._wait_for_leg2_confirmation_or_autoconfirm(cycle)

        if not confirmed:
            logger.warning(
                "CYCLE LEG2 | %s | Withdrawal not confirmed, failing cycle",
                cycle.cycle_id,
            )
            return False

        # Transition to LEG2 execution
        self._transition_state(cycle, CycleState.LEG2)

        # Perform withdrawal
        success = await self._perform_withdrawal(
            cycle,
            opp,
            withdrawal_amount,
            network,
            withdrawal_fee,
        )

        # Finalize execution
        self._finalize_leg2_execution(
            cycle,
            opp,
            withdrawal_amount,
            withdrawal_fee,
            success,
        )

        return success

    async def _execute_leg3(self, cycle: TradeCycle) -> bool:
        """
        Execute LEG3: Sell order with market order fallback.

        Delegates to LegExecutor.

        Args:
            cycle: TradeCycle to execute

        Returns:
            True if successful (order filled), False if failed
        """
        return await self.leg_executor.execute_leg3(cycle)

    async def _complete_cycle(self, cycle: TradeCycle) -> None:
        """
        Complete a cycle successfully.

        Args:
            cycle: TradeCycle to complete
        """
        self._transition_state(cycle, CycleState.COMPLETE)
        cycle.completed_at = datetime.utcnow()
        cycle.log_event(
            CycleState.COMPLETE,
            "Cycle completed successfully",
            {
                "duration_seconds": cycle.duration_seconds(),
                "realized_profit_usd": cycle.realized_profit_usd,
                "realized_profit_pct": cycle.realized_profit_pct,
            },
        )

        # Log to file
        self.trade_logger.log_cycle(cycle)

        # Update statistics
        self.total_cycles_completed += 1

        # Remove from active cycles
        if cycle.cycle_id in self.active_cycles:
            del self.active_cycles[cycle.cycle_id]

        logger.info(
            "CYCLE COMPLETE | %s | duration=%.2fs profit=%.2f%% (%.2f USD)",
            cycle.cycle_id,
            cycle.duration_seconds(),
            cycle.realized_profit_pct,
            cycle.realized_profit_usd,
        )

    async def _fail_cycle(self, cycle: TradeCycle, reason: str) -> None:
        """
        Fail a cycle.

        Args:
            cycle: TradeCycle to fail
            reason: Failure reason
        """
        self._transition_state(cycle, CycleState.FAILED)
        cycle.completed_at = datetime.utcnow()
        cycle.failure_reason = reason
        cycle.log_event(
            CycleState.FAILED,
            f"Cycle failed: {reason}",
            {"duration_seconds": cycle.duration_seconds()},
        )

        # Log to file
        self.trade_logger.log_cycle(cycle)

        # Update statistics
        self.total_cycles_failed += 1

        # Remove from active cycles
        if cycle.cycle_id in self.active_cycles:
            del self.active_cycles[cycle.cycle_id]

        logger.warning(
            "CYCLE FAILED | %s | reason=%s duration=%.2fs",
            cycle.cycle_id,
            reason,
            cycle.duration_seconds(),
        )

    def _transition_state(self, cycle: TradeCycle, new_state: CycleState) -> None:
        """
        Transition cycle to a new state.

        Validates state transitions and logs the change.

        Args:
            cycle: TradeCycle to transition
            new_state: New CycleState
        """
        old_state = cycle.state
        cycle.state = new_state

        logger.debug(
            "CYCLE STATE | %s | %s → %s",
            cycle.cycle_id,
            old_state.value,
            new_state.value,
        )

    def get_active_cycle_ids(self) -> list[str]:
        """
        Get list of active cycle IDs.

        Returns:
            List of active cycle IDs
        """
        return list(self.active_cycles.keys())

    def get_cycle(self, cycle_id: str) -> Optional[TradeCycle]:
        """
        Get a cycle by ID.

        Args:
            cycle_id: Cycle ID to retrieve

        Returns:
            TradeCycle if found, None otherwise
        """
        return self.active_cycles.get(cycle_id)

    def get_stats(self) -> dict:
        """
        Get trader statistics.

        Returns:
            Dict with statistics
        """
        return {
            "active_cycles": len(self.active_cycles),
            "total_started": self.total_cycles_started,
            "total_completed": self.total_cycles_completed,
            "total_failed": self.total_cycles_failed,
            "success_rate": (
                self.total_cycles_completed / self.total_cycles_started * 100.0
                if self.total_cycles_started > 0
                else 0.0
            ),
        }

    async def cancel_cycle(self, cycle_id: str, reason: str = "User cancelled") -> bool:
        """
        Cancel an active cycle.

        Args:
            cycle_id: Cycle ID to cancel
            reason: Cancellation reason

        Returns:
            True if cancelled, False if not found
        """
        cycle = self.active_cycles.get(cycle_id)
        if not cycle:
            logger.warning("CYCLE CANCEL | %s | Not found", cycle_id)
            return False

        self._transition_state(cycle, CycleState.CANCELLED)
        cycle.completed_at = datetime.utcnow()
        cycle.failure_reason = reason
        cycle.log_event(CycleState.CANCELLED, f"Cycle cancelled: {reason}")

        # Log to file
        self.trade_logger.log_cycle(cycle)

        # Remove from active cycles
        del self.active_cycles[cycle_id]

        logger.info("CYCLE CANCELLED | %s | reason=%s", cycle_id, reason)
        return True

    async def confirm_leg2_withdrawal(self, cycle_id: str) -> bool:
        """
        Confirm LEG2 withdrawal via Telegram command.

        This method is called by the Telegram bot when a user
        sends /confirm <cycle_id>.

        Args:
            cycle_id: Cycle ID to confirm

        Returns:
            True if confirmed, False if not found or wrong state
        """
        cycle = self.active_cycles.get(cycle_id)
        if not cycle:
            logger.warning("CYCLE LEG2 CONFIRM | %s | Not found", cycle_id)
            return False

        if cycle.state != CycleState.LEG2_WAIT:
            logger.warning(
                "CYCLE LEG2 CONFIRM | %s | Wrong state: %s (expected LEG2_WAIT)",
                cycle_id,
                cycle.state.value,
            )
            return False

        # Delegate to TelegramLeg2Handler
        return self.telegram_handler.confirm(cycle_id)

    async def _wait_for_leg2_confirmation(
        self, cycle: TradeCycle, timeout_seconds: int = 300
    ) -> bool:
        """
        Wait for LEG2 withdrawal confirmation from Telegram.

        Args:
            cycle: TradeCycle waiting for confirmation
            timeout_seconds: Maximum wait time (default: 5 minutes)

        Returns:
            True if confirmed within timeout, False if timeout
        """
        # Delegate to TelegramLeg2Handler
        confirmed = await self.telegram_handler.wait_for_confirmation(
            cycle.cycle_id, timeout_seconds
        )

        if not confirmed:
            cycle.log_event(
                CycleState.LEG2_WAIT,
                f"Confirmation timeout after {timeout_seconds}s",
            )

        return confirmed

    async def _send_leg2_notification(self, cycle: TradeCycle) -> None:
        """
        Send Telegram notification when cycle reaches LEG2_WAIT.

        Args:
            cycle: TradeCycle in LEG2_WAIT state
        """
        # Delegate to TelegramLeg2Handler
        await self.telegram_handler.send_leg2_notification(cycle)

    async def shutdown(self) -> None:
        """
        Gracefully shutdown trader.

        Cancels all active cycles, background tasks, and logs statistics.
        """
        logger.info("SimpleTrader shutdown initiated")

        # Cancel all active cycles
        for cycle_id in list(self.active_cycles.keys()):
            await self.cancel_cycle(cycle_id, "Trader shutdown")

        # Cancel all background tasks
        if self._background_tasks:
            logger.info("Cancelling %d background tasks", len(self._background_tasks))
            for task in self._background_tasks:
                task.cancel()
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        # Log final statistics
        stats = self.get_stats()
        logger.info(
            "SimpleTrader shutdown complete: started=%d completed=%d failed=%d",
            stats["total_started"],
            stats["total_completed"],
            stats["total_failed"],
        )
