"""
Unit tests for TelegramLeg2Handler (trader_telegram.py).

Tests the Telegram confirmation handling in isolation.
"""

import pytest
from unittest.mock import MagicMock
import asyncio

from parsertang.trader_telegram import TelegramLeg2Handler


@pytest.fixture
def handler():
    """Create a TelegramLeg2Handler without alert service."""
    return TelegramLeg2Handler()


@pytest.fixture
def handler_with_alert():
    """Create a TelegramLeg2Handler with mock alert service."""
    mock_alert = MagicMock()
    mock_alert.send = MagicMock()
    return TelegramLeg2Handler(alert_service=mock_alert)


@pytest.fixture
def mock_cycle():
    """Create a mock TradeCycle."""
    cycle = MagicMock()
    cycle.cycle_id = "test-cycle-001"
    cycle.position_amount = 0.1
    cycle.base_currency = "BTC"
    cycle.opportunity = MagicMock()
    cycle.opportunity.symbol = "BTC/USDT"
    cycle.opportunity.buy_exchange = "binance"
    cycle.opportunity.sell_exchange = "kucoin"
    cycle.opportunity.network = "TRC20"
    cycle.opportunity.net_profit_pct = 0.5
    return cycle


class TestTelegramLeg2HandlerConfirm:
    """Tests for confirm method."""

    def test_confirm_no_pending_event(self, handler):
        """Test confirming when no event exists."""
        result = handler.confirm("nonexistent-cycle")
        assert result is False

    @pytest.mark.asyncio
    async def test_confirm_with_pending_event(self, handler):
        """Test confirming when event exists."""

        # Start waiting in background
        async def wait_task():
            return await handler.wait_for_confirmation("test-cycle", timeout_seconds=5)

        task = asyncio.create_task(wait_task())
        await asyncio.sleep(0.01)  # Let it start

        # Confirm
        result = handler.confirm("test-cycle")
        assert result is True

        # Wait task should complete successfully
        wait_result = await task
        assert wait_result is True

    def test_has_pending_confirmation(self, handler):
        """Test has_pending_confirmation method."""
        assert handler.has_pending_confirmation("test-cycle") is False

        # Create an event
        handler._confirmation_events["test-cycle"] = asyncio.Event()
        assert handler.has_pending_confirmation("test-cycle") is True


class TestTelegramLeg2HandlerWait:
    """Tests for wait_for_confirmation method."""

    @pytest.mark.asyncio
    async def test_wait_timeout(self, handler):
        """Test waiting for confirmation with timeout."""
        result = await handler.wait_for_confirmation(
            "test-cycle-timeout",
            timeout_seconds=0.01,  # Very short timeout
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_wait_cleanup_on_timeout(self, handler):
        """Test that event is cleaned up after timeout."""
        await handler.wait_for_confirmation(
            "cleanup-test",
            timeout_seconds=0.01,
        )
        # Event should be cleaned up
        assert "cleanup-test" not in handler._confirmation_events

    @pytest.mark.asyncio
    async def test_wait_with_callback(self):
        """Test on_confirmed callback is called."""
        callback_called = []

        async def on_confirmed(cycle_id):
            callback_called.append(cycle_id)

        handler = TelegramLeg2Handler(on_confirmed=on_confirmed)

        # Start waiting
        async def wait_task():
            return await handler.wait_for_confirmation(
                "callback-test", timeout_seconds=1
            )

        task = asyncio.create_task(wait_task())
        await asyncio.sleep(0.01)

        # Confirm
        handler.confirm("callback-test")
        await task

        assert "callback-test" in callback_called


class TestTelegramLeg2HandlerNotification:
    """Tests for send_leg2_notification method."""

    @pytest.mark.asyncio
    async def test_send_notification_no_alert_service(self, handler, mock_cycle):
        """Test sending notification without alert service (no-op)."""
        # Should not raise
        await handler.send_leg2_notification(mock_cycle)

    @pytest.mark.asyncio
    async def test_send_notification_with_alert_service(
        self, handler_with_alert, mock_cycle
    ):
        """Test sending notification with alert service."""
        await handler_with_alert.send_leg2_notification(mock_cycle)

        # Alert service should have been called
        handler_with_alert.alert_service.send.assert_called_once()

        # Check message contains expected content
        call_args = handler_with_alert.alert_service.send.call_args[0][0]
        assert "test-cycle-001" in call_args
        assert "BTC/USDT" in call_args
        assert "/confirm" in call_args

    @pytest.mark.asyncio
    async def test_send_notification_no_opportunity(
        self, handler_with_alert, mock_cycle
    ):
        """Test sending notification when cycle has no opportunity."""
        mock_cycle.opportunity = None
        await handler_with_alert.send_leg2_notification(mock_cycle)

        # Should not call alert service
        handler_with_alert.alert_service.send.assert_not_called()
