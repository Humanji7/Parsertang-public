"""
Tests for Phase R3 Telegram integration.

Tests the LEG2 confirmation flow:
- confirm_leg2_withdrawal() method
- _wait_for_leg2_confirmation() timeout handling
- Telegram notification sending
- State transitions with confirmation
"""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

from parsertang.arbitrage import Opportunity
from parsertang.trade_models import CycleState, TradeCycle
from parsertang.trader import SimpleTrader


@pytest.fixture
def mock_opportunity():
    """Create a mock arbitrage opportunity."""
    return Opportunity(
        symbol="BTC/USDT",
        buy_exchange="binance",
        buy_price=45000.0,
        sell_exchange="bybit",
        sell_price=45500.0,
        gross_spread_pct=1.11,
        trade_fees_pct=0.20,
        withdraw_fee_pct=0.01,
        net_profit_pct=0.90,
        bid_liq_usd=80000.0,
        ask_liq_usd=90000.0,
        network="TRC20",
    )


@pytest.fixture
def trader():
    """Create a SimpleTrader instance for testing."""
    return SimpleTrader(
        dry_run=True,
        max_concurrent_cycles=3,
    )


@pytest.fixture
def trader_with_alert():
    """Create a SimpleTrader with mock alert service."""
    mock_alert = Mock()
    mock_alert.send = Mock()

    return SimpleTrader(
        dry_run=True,
        max_concurrent_cycles=3,
        alert_service=mock_alert,
    )


def test_trader_initialization_with_alert_service():
    """Test that trader initializes with alert service."""
    mock_alert = Mock()
    trader = SimpleTrader(
        dry_run=True,
        alert_service=mock_alert,
    )

    assert trader.alert_service is mock_alert
    # Confirmation events are now managed by TelegramLeg2Handler (composition pattern)
    assert trader.telegram_handler._confirmation_events == {}


@pytest.mark.asyncio
async def test_confirm_leg2_withdrawal_success(trader_with_alert, mock_opportunity):
    """Test successful LEG2 confirmation."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT
    cycle.position_amount = 0.001

    # Add to active cycles
    trader_with_alert.active_cycles[cycle.cycle_id] = cycle

    # Create confirmation event (now managed by TelegramLeg2Handler)
    event = asyncio.Event()
    trader_with_alert.telegram_handler._confirmation_events[cycle.cycle_id] = event

    # Confirm
    success = await trader_with_alert.confirm_leg2_withdrawal(cycle.cycle_id)

    assert success is True
    assert event.is_set()


@pytest.mark.asyncio
async def test_confirm_leg2_withdrawal_cycle_not_found(trader):
    """Test LEG2 confirmation with non-existent cycle."""
    success = await trader.confirm_leg2_withdrawal("nonexistent")
    assert success is False


@pytest.mark.asyncio
async def test_confirm_leg2_withdrawal_wrong_state(trader, mock_opportunity):
    """Test LEG2 confirmation with wrong cycle state."""
    # Create cycle in LEG1 state
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG1

    # Add to active cycles
    trader.active_cycles[cycle.cycle_id] = cycle

    # Try to confirm
    success = await trader.confirm_leg2_withdrawal(cycle.cycle_id)

    assert success is False


@pytest.mark.asyncio
async def test_wait_for_leg2_confirmation_success(trader, mock_opportunity):
    """Test waiting for confirmation with successful confirmation."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT

    # Start waiting in background
    wait_task = asyncio.create_task(
        trader._wait_for_leg2_confirmation(cycle, timeout_seconds=5)
    )

    # Wait a bit then confirm
    await asyncio.sleep(0.1)

    # Simulate confirmation (events now managed by TelegramLeg2Handler)
    assert cycle.cycle_id in trader.telegram_handler._confirmation_events
    trader.telegram_handler._confirmation_events[cycle.cycle_id].set()

    # Check result
    result = await wait_task
    assert result is True

    # Event should be cleaned up
    assert cycle.cycle_id not in trader.telegram_handler._confirmation_events


@pytest.mark.asyncio
async def test_wait_for_leg2_confirmation_timeout(trader, mock_opportunity):
    """Test waiting for confirmation with timeout."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT

    # Wait with short timeout
    result = await trader._wait_for_leg2_confirmation(cycle, timeout_seconds=0.1)

    assert result is False

    # Event should be cleaned up (events now managed by TelegramLeg2Handler)
    assert cycle.cycle_id not in trader.telegram_handler._confirmation_events

    # Check that timeout event was logged
    timeout_events = [e for e in cycle.events if "timeout" in e.message.lower()]
    assert len(timeout_events) > 0


@pytest.mark.asyncio
async def test_send_leg2_notification_no_alert_service(trader, mock_opportunity):
    """Test notification sending without alert service."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT
    cycle.position_amount = 0.001
    cycle.base_currency = "BTC"

    # Should not raise error
    await trader._send_leg2_notification(cycle)


@pytest.mark.asyncio
async def test_send_leg2_notification_with_alert_service(
    trader_with_alert, mock_opportunity
):
    """Test notification sending with alert service."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT
    cycle.position_amount = 0.001
    cycle.base_currency = "BTC"

    # Mock asyncio.to_thread to avoid actual threading
    with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread:
        mock_to_thread.return_value = None

        # Send notification
        await trader_with_alert._send_leg2_notification(cycle)

        # Verify alert service was called
        mock_to_thread.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_cycle_in_leg2_wait(trader, mock_opportunity):
    """Test cancelling a cycle in LEG2_WAIT state."""
    # Create cycle
    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT
    cycle.started_at = datetime.utcnow()

    # Add to active cycles
    trader.active_cycles[cycle.cycle_id] = cycle

    # Cancel
    success = await trader.cancel_cycle(cycle.cycle_id, reason="User cancelled")

    assert success is True
    assert cycle.cycle_id not in trader.active_cycles
    assert cycle.state == CycleState.CANCELLED
    assert cycle.failure_reason == "User cancelled"
    assert cycle.completed_at is not None


@pytest.mark.asyncio
async def test_leg2_execution_phase_r2_auto_confirm(trader, mock_opportunity):
    """Test LEG2 execution in Phase R2 (auto-confirm mode)."""
    from parsertang.config import settings

    # Set Phase R2
    original_phase = settings.current_phase
    settings.current_phase = "R2"

    try:
        # Create cycle
        cycle = TradeCycle(opportunity=mock_opportunity)
        cycle.state = CycleState.LEG1
        cycle.position_amount = 0.001
        cycle.base_currency = "BTC"

        # Add to active cycles
        trader.active_cycles[cycle.cycle_id] = cycle

        # Mock withdrawal execution to always succeed (avoid flaky test)
        # _perform_withdrawal delegates to simulator.simulate_withdrawal
        with patch.object(
            trader, "_perform_withdrawal", new_callable=AsyncMock, return_value=True
        ):
            # Execute LEG2
            success = await trader._execute_leg2(cycle)

            # Should succeed without waiting for confirmation
            assert success is True
            assert cycle.state == CycleState.LEG2

    finally:
        # Restore original phase
        settings.current_phase = original_phase


@pytest.mark.asyncio
async def test_leg2_execution_phase_r3_with_confirmation(
    trader_with_alert, mock_opportunity
):
    """Test LEG2 execution in Phase R3 with confirmation."""
    from parsertang.config import settings

    # Set Phase R3
    original_phase = settings.current_phase
    settings.current_phase = "R3"

    try:
        # Create cycle
        cycle = TradeCycle(opportunity=mock_opportunity)
        cycle.state = CycleState.LEG1
        cycle.position_amount = 0.001
        cycle.base_currency = "BTC"

        # Add to active cycles
        trader_with_alert.active_cycles[cycle.cycle_id] = cycle

        # Mock notification sending and withdrawal execution
        # _perform_withdrawal delegates to simulator.simulate_withdrawal
        with (
            patch.object(
                trader_with_alert, "_send_leg2_notification", new_callable=AsyncMock
            ) as mock_notify,
            patch.object(
                trader_with_alert,
                "_perform_withdrawal",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            # Start LEG2 execution in background
            leg2_task = asyncio.create_task(trader_with_alert._execute_leg2(cycle))

            # Wait for notification to be sent
            await asyncio.sleep(0.2)
            mock_notify.assert_called_once()

            # Check that cycle is waiting for confirmation (events now in TelegramLeg2Handler)
            assert (
                cycle.cycle_id
                in trader_with_alert.telegram_handler._confirmation_events
            )

            # Confirm
            await trader_with_alert.confirm_leg2_withdrawal(cycle.cycle_id)

            # Wait for completion
            success = await leg2_task

            # Should succeed
            assert success is True

    finally:
        # Restore original phase
        settings.current_phase = original_phase


@pytest.mark.asyncio
async def test_leg2_execution_phase_r3_timeout(trader_with_alert, mock_opportunity):
    """Test LEG2 execution in Phase R3 with timeout (no confirmation)."""
    from parsertang.config import settings

    # Set Phase R3
    original_phase = settings.current_phase
    settings.current_phase = "R3"

    try:
        # Create cycle
        cycle = TradeCycle(opportunity=mock_opportunity)
        cycle.state = CycleState.LEG1
        cycle.position_amount = 0.001
        cycle.base_currency = "BTC"

        # Add to active cycles
        trader_with_alert.active_cycles[cycle.cycle_id] = cycle

        # Mock notification sending and set very short timeout
        with patch.object(
            trader_with_alert, "_send_leg2_notification", new_callable=AsyncMock
        ):
            with patch.object(
                trader_with_alert, "_wait_for_leg2_confirmation", return_value=False
            ):
                # Execute LEG2 (should timeout)
                success = await trader_with_alert._execute_leg2(cycle)

                # Should fail due to timeout
                assert success is False

    finally:
        # Restore original phase
        settings.current_phase = original_phase


def test_get_stats_includes_active_cycles(trader, mock_opportunity):
    """Test that statistics include active cycles count."""
    # Add some cycles
    cycle1 = TradeCycle(opportunity=mock_opportunity)
    cycle2 = TradeCycle(opportunity=mock_opportunity)

    trader.active_cycles[cycle1.cycle_id] = cycle1
    trader.active_cycles[cycle2.cycle_id] = cycle2

    stats = trader.get_stats()

    assert stats["active_cycles"] == 2
    assert "total_started" in stats
    assert "total_completed" in stats
    assert "total_failed" in stats
    assert "success_rate" in stats
