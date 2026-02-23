"""
TelegramLeg2Handler - Telegram confirmation handling for LEG2 withdrawals.

This module extracts Telegram-related functionality from SimpleTrader:
- LEG2 confirmation waiting (asyncio Event-based)
- Telegram notification sending via AlertService
- Confirmation API for Telegram bot callback

Architecture: Composition pattern (NOT mixin).
Handler is instantiated with callbacks for confirmed/timeout events.
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Callable, Awaitable, Optional, Dict

if TYPE_CHECKING:
    from parsertang.alerts import AlertService
    from parsertang.trade_models import TradeCycle

logger = logging.getLogger(__name__)

# Default timeout for LEG2 confirmation (5 minutes)
DEFAULT_LEG2_CONFIRMATION_TIMEOUT = 300


class TelegramLeg2Handler:
    """
    Handles LEG2 Telegram confirmation flow.

    Manages the confirmation Events and notification sending,
    but delegates actual state transitions to the caller via callbacks.

    Usage:
        handler = TelegramLeg2Handler(alert_service, on_confirmed=..., on_timeout=...)
        await handler.wait_for_confirmation(cycle)
    """

    def __init__(
        self,
        alert_service: Optional["AlertService"] = None,
        on_confirmed: Optional[Callable[[str], Awaitable[None]]] = None,
        on_timeout: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        """
        Initialize TelegramLeg2Handler.

        Args:
            alert_service: AlertService for sending Telegram notifications
            on_confirmed: Async callback when confirmation received (cycle_id)
            on_timeout: Async callback when confirmation timeout (cycle_id)
        """
        self.alert_service = alert_service
        self.on_confirmed = on_confirmed
        self.on_timeout = on_timeout

        # Confirmation events keyed by cycle_id
        self._confirmation_events: Dict[str, asyncio.Event] = {}

    def confirm(self, cycle_id: str) -> bool:
        """
        Confirm LEG2 withdrawal (called by Telegram bot).

        This method is called by the Telegram bot when a user
        sends /confirm <cycle_id>.

        Args:
            cycle_id: Cycle ID to confirm

        Returns:
            True if confirmation event was set, False if not found
        """
        if cycle_id not in self._confirmation_events:
            logger.warning(
                "CYCLE LEG2 CONFIRM | %s | No confirmation event found",
                cycle_id,
            )
            return False

        self._confirmation_events[cycle_id].set()
        logger.info("CYCLE LEG2 CONFIRM | %s | Confirmation received", cycle_id)
        return True

    async def wait_for_confirmation(
        self, cycle_id: str, timeout_seconds: int = DEFAULT_LEG2_CONFIRMATION_TIMEOUT
    ) -> bool:
        """
        Wait for LEG2 withdrawal confirmation from Telegram.

        Args:
            cycle_id: Cycle ID waiting for confirmation
            timeout_seconds: Maximum wait time (default: 5 minutes)

        Returns:
            True if confirmed within timeout, False if timeout
        """
        # Create confirmation event
        event = asyncio.Event()
        self._confirmation_events[cycle_id] = event

        try:
            # Wait for confirmation with timeout
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
            logger.info(
                "CYCLE LEG2 | %s | Confirmation received within timeout",
                cycle_id,
            )

            # Call on_confirmed callback
            if self.on_confirmed:
                await self.on_confirmed(cycle_id)

            return True
        except asyncio.TimeoutError:
            logger.warning(
                "CYCLE LEG2 | %s | Confirmation timeout after %ds",
                cycle_id,
                timeout_seconds,
            )

            # Call on_timeout callback
            if self.on_timeout:
                await self.on_timeout(cycle_id)

            return False
        finally:
            # Clean up event
            if cycle_id in self._confirmation_events:
                del self._confirmation_events[cycle_id]

    async def send_leg2_notification(self, cycle: "TradeCycle") -> None:
        """
        Send Telegram notification when cycle reaches LEG2_WAIT.

        Args:
            cycle: TradeCycle in LEG2_WAIT state
        """
        if not self.alert_service:
            logger.debug(
                "CYCLE LEG2 | %s | No alert service, skipping notification",
                cycle.cycle_id,
            )
            return

        opp = cycle.opportunity
        if not opp:
            return

        # Build notification message
        message = (
            f"🔔 LEG2 Withdrawal Confirmation Required\n\n"
            f"Cycle: <code>{cycle.cycle_id}</code>\n"
            f"Symbol: <b>{opp.symbol}</b>\n"
            f"From: {opp.buy_exchange}\n"
            f"To: {opp.sell_exchange}\n"
            f"Amount: {cycle.position_amount:.8f} {cycle.base_currency}\n"
            f"Network: {opp.network or 'N/A'}\n"
            f"Expected Profit: <b>{opp.net_profit_pct:.2f}%</b>\n\n"
            f"Use /confirm {cycle.cycle_id} to approve\n"
            f"Use /cancel {cycle.cycle_id} to cancel"
        )

        try:
            # Send via AlertService
            await asyncio.to_thread(self.alert_service.send, message)
            logger.info(
                "CYCLE LEG2 | %s | Telegram notification sent",
                cycle.cycle_id,
            )
        except Exception as e:
            logger.error(
                "CYCLE LEG2 | %s | Failed to send Telegram notification: %s",
                cycle.cycle_id,
                e,
            )

    def has_pending_confirmation(self, cycle_id: str) -> bool:
        """
        Check if a cycle has a pending confirmation event.

        Args:
            cycle_id: Cycle ID to check

        Returns:
            True if there is a pending confirmation event
        """
        return cycle_id in self._confirmation_events
