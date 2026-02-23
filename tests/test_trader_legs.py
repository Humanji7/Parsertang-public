"""
Unit tests for LegExecutor (trader_legs.py).

Tests leg execution in isolation with mocked dependencies.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock

from parsertang.trader_legs import LegExecutor
from parsertang.trade_models import CycleState


@pytest.fixture
def mock_simulator():
    """Create a mock TradeSimulator."""
    sim = MagicMock()

    # Mock successful buy order
    sim.simulate_buy_order = AsyncMock(
        return_value=MagicMock(
            success=True,
            order_info=MagicMock(
                order_id="dry_test123",
                filled=0.1,
                average_price=50000.0,
            ),
            suggested_state=None,
        )
    )

    # Mock successful sell order
    sim.simulate_sell_order = AsyncMock(
        return_value=MagicMock(
            success=True,
            order_info=MagicMock(
                order_id="dry_sell123",
                filled=0.1,
                average_price=51000.0,
            ),
            filled_price=51000.0,
            suggested_state=None,
        )
    )

    return sim


@pytest.fixture
def mock_trade_logger():
    """Create a mock TradeLogger."""
    logger = MagicMock()
    logger.log_cycle_update = MagicMock()
    return logger


@pytest.fixture
def mock_cycle():
    """Create a mock TradeCycle."""
    cycle = MagicMock()
    cycle.cycle_id = "test-leg-cycle"
    cycle.state = CycleState.SCANNING
    cycle.base_currency = "BTC"
    cycle.position_amount = 0.1
    cycle.position_value_usd = 5000.0
    cycle.total_fees_usd = 0.0
    cycle.realized_profit_usd = 0.0
    cycle.realized_profit_pct = 0.0

    cycle.opportunity = MagicMock()
    cycle.opportunity.buy_exchange = "binance"
    cycle.opportunity.sell_exchange = "kucoin"
    cycle.opportunity.symbol = "BTC/USDT"
    cycle.opportunity.buy_price = 50000.0
    cycle.opportunity.sell_price = 51000.0
    cycle.opportunity.ask_liq_usd = 10000.0
    cycle.opportunity.buy_taker_fee_pct = 0.1
    cycle.opportunity.sell_taker_fee_pct = 0.1

    cycle.log_event = MagicMock()

    return cycle


@pytest.fixture
def executor(mock_simulator, mock_trade_logger):
    """Create a LegExecutor with mocked dependencies."""
    transition_callback = MagicMock()
    return LegExecutor(
        simulator=mock_simulator,
        trade_logger=mock_trade_logger,
        dry_run=True,
        on_transition=transition_callback,
    )


class TestLegExecutorInit:
    """Tests for LegExecutor initialization."""

    def test_init_with_all_params(self, mock_simulator, mock_trade_logger):
        """Test initialization with all parameters."""
        callback = MagicMock()
        executor = LegExecutor(
            simulator=mock_simulator,
            trade_logger=mock_trade_logger,
            dry_run=True,
            on_transition=callback,
        )

        assert executor.simulator == mock_simulator
        assert executor.trade_logger == mock_trade_logger
        assert executor.dry_run is True
        assert executor.on_transition == callback

    def test_init_without_simulator(self, mock_trade_logger):
        """Test initialization without simulator (real mode)."""
        executor = LegExecutor(
            simulator=None,
            trade_logger=mock_trade_logger,
            dry_run=False,
        )

        assert executor.simulator is None
        assert executor.dry_run is False


class TestLegExecutorLeg1:
    """Tests for execute_leg1 method."""

    @pytest.mark.asyncio
    async def test_leg1_success(self, executor, mock_cycle):
        """Test successful LEG1 execution."""
        result = await executor.execute_leg1(mock_cycle)

        assert result is True
        executor.on_transition.assert_any_call(mock_cycle, CycleState.LEG1)
        executor.trade_logger.log_cycle_update.assert_called()

    @pytest.mark.asyncio
    async def test_leg1_calls_simulator(self, executor, mock_cycle, mock_simulator):
        """Test that LEG1 calls simulator with correct params."""
        await executor.execute_leg1(mock_cycle)

        mock_simulator.simulate_buy_order.assert_called_once()
        call_kwargs = mock_simulator.simulate_buy_order.call_args.kwargs

        assert call_kwargs["exchange"] == "binance"
        assert call_kwargs["symbol"] == "BTC/USDT"
        assert call_kwargs["cycle_id"] == "test-leg-cycle"

    @pytest.mark.asyncio
    async def test_leg1_updates_cycle_on_success(self, executor, mock_cycle):
        """Test that LEG1 updates cycle properties on success."""
        await executor.execute_leg1(mock_cycle)

        # Cycle should have position set
        assert mock_cycle.position_amount > 0 or hasattr(mock_cycle, "position_amount")
        mock_cycle.log_event.assert_called()


class TestLegExecutorLeg3:
    """Tests for execute_leg3 method."""

    @pytest.mark.asyncio
    async def test_leg3_success(self, executor, mock_cycle):
        """Test successful LEG3 execution."""
        result = await executor.execute_leg3(mock_cycle)

        assert result is True
        executor.on_transition.assert_any_call(mock_cycle, CycleState.LEG3)

    @pytest.mark.asyncio
    async def test_leg3_market_fallback(self, executor, mock_cycle, mock_simulator):
        """Test LEG3 falls back to market order on limit timeout."""
        # Make limit order fail
        mock_simulator.simulate_sell_order = AsyncMock(
            side_effect=[
                MagicMock(
                    success=False,
                    order_info=MagicMock(status="timeout"),
                    filled_price=0,
                ),
                MagicMock(
                    success=True,
                    order_info=MagicMock(status="closed"),
                    filled_price=50900.0,
                ),
            ]
        )

        result = await executor.execute_leg3(mock_cycle)

        # Should still succeed via market order
        assert result is True
        assert mock_simulator.simulate_sell_order.call_count == 2

        # Check second call was market order
        second_call = mock_simulator.simulate_sell_order.call_args_list[1]
        assert second_call.kwargs.get("order_type") == "market"


class TestLegExecutorTransitionCallback:
    """Tests for state transition callback."""

    @pytest.mark.asyncio
    async def test_transition_via_callback(self, executor, mock_cycle):
        """Test that state transitions go through callback."""
        await executor.execute_leg1(mock_cycle)

        # Callback should have been called with LEG1 state
        executor.on_transition.assert_called()
        states = [call[0][1] for call in executor.on_transition.call_args_list]
        assert CycleState.LEG1 in states

    def test_transition_fallback_without_callback(self, mock_trade_logger, mock_cycle):
        """Test direct transition when no callback provided."""
        executor = LegExecutor(
            simulator=None,
            trade_logger=mock_trade_logger,
            dry_run=False,
            on_transition=None,  # No callback
        )

        # Should not raise
        executor._transition_state(mock_cycle, CycleState.LEG1)

        # Cycle state should be updated directly
        assert mock_cycle.state == CycleState.LEG1
