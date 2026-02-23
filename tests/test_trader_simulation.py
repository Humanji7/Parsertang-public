"""
Unit tests for TradeSimulator (trader_simulation.py).

Tests the stateless simulation components in isolation.
"""

import pytest
from unittest.mock import patch

from parsertang.trader_simulation import (
    TradeSimulator,
    SimulationResult,
)
from parsertang.trade_models import CycleState


@pytest.fixture
def simulator():
    """Create a TradeSimulator instance."""
    return TradeSimulator()


class TestSimulationResult:
    """Tests for SimulationResult dataclass."""

    def test_success_result(self):
        """Test creating a successful result."""
        result = SimulationResult(success=True)
        assert result.success is True
        assert result.error is None
        assert result.suggested_state is None

    def test_failure_result_with_error(self):
        """Test creating a failure result with error message."""
        result = SimulationResult(
            success=False,
            error="Order timeout",
            suggested_state=CycleState.LEG1_TIMEOUT,
        )
        assert result.success is False
        assert result.error == "Order timeout"
        assert result.suggested_state == CycleState.LEG1_TIMEOUT


class TestTradeSimulatorBuyOrder:
    """Tests for simulate_buy_order method."""

    @pytest.mark.asyncio
    async def test_buy_order_success(self, simulator):
        """Test successful buy order simulation."""
        with patch("random.random", return_value=0.5):  # < 0.9 = success
            result = await simulator.simulate_buy_order(
                exchange="binance",
                symbol="BTC/USDT",
                price=50000.0,
                amount=0.1,
                cycle_id="test-cycle-001",
            )

        assert result.success is True
        assert result.order_info is not None
        assert result.order_info.status == "closed"
        assert result.order_info.filled == 0.1
        assert result.order_info.average_price == 50000.0
        assert result.suggested_state is None

    @pytest.mark.asyncio
    async def test_buy_order_timeout(self, simulator):
        """Test buy order timeout simulation."""
        with patch("random.random", return_value=0.95):  # > 0.9 = timeout
            with patch("parsertang.config.settings.leg1_timeout_seconds", 0.01):
                result = await simulator.simulate_buy_order(
                    exchange="binance",
                    symbol="BTC/USDT",
                    price=50000.0,
                    amount=0.1,
                    cycle_id="test-cycle-002",
                )

        assert result.success is False
        assert result.order_info is not None
        assert result.order_info.status == "timeout"
        assert result.suggested_state == CycleState.LEG1_TIMEOUT
        assert "timeout" in result.error.lower()

    @pytest.mark.asyncio
    async def test_buy_order_info_structure(self, simulator):
        """Test that order info has correct structure."""
        with patch("random.random", return_value=0.5):
            result = await simulator.simulate_buy_order(
                exchange="kucoin",
                symbol="ETH/USDT",
                price=3000.0,
                amount=1.0,
                cycle_id="test-cycle-003",
            )

        order = result.order_info
        assert order.order_type == "limit"
        assert order.side == "buy"
        assert order.order_id.startswith("dry_")


class TestTradeSimulatorWithdrawal:
    """Tests for simulate_withdrawal method."""

    @pytest.mark.asyncio
    async def test_withdrawal_success(self, simulator):
        """Test successful withdrawal simulation."""
        with patch("random.random", return_value=0.5):  # < 0.95 = success
            result = await simulator.simulate_withdrawal(
                from_exchange="binance",
                to_exchange="kucoin",
                currency="USDT",
                amount=1000.0,
                network="TRC20",
                fee=1.0,
                cycle_id="test-cycle-004",
            )

        assert result.success is True
        assert result.withdrawal_info is not None
        assert result.withdrawal_info.status == "completed"
        assert result.withdrawal_info.currency == "USDT"

    @pytest.mark.asyncio
    async def test_withdrawal_failure(self, simulator):
        """Test failed withdrawal simulation."""
        with patch("random.random", return_value=0.99):  # > 0.95 = failure
            result = await simulator.simulate_withdrawal(
                from_exchange="binance",
                to_exchange="kucoin",
                currency="BTC",
                amount=0.1,
                network="BTC",
                fee=0.0001,
                cycle_id="test-cycle-005",
            )

        assert result.success is False
        assert result.withdrawal_info is not None
        assert result.withdrawal_info.status == "failed"


class TestTradeSimulatorSellOrder:
    """Tests for simulate_sell_order method."""

    @pytest.mark.asyncio
    async def test_limit_sell_success(self, simulator):
        """Test successful limit sell order."""
        with patch("random.random", return_value=0.5):  # < 0.7 = success
            result = await simulator.simulate_sell_order(
                exchange="kucoin",
                symbol="BTC/USDT",
                price=51000.0,
                amount=0.1,
                order_type="limit",
                cycle_id="test-cycle-006",
            )

        assert result.success is True
        assert result.filled_price == 51000.0
        assert result.order_info.status == "closed"

    @pytest.mark.asyncio
    async def test_limit_sell_timeout(self, simulator):
        """Test limit sell order timeout."""
        with patch("random.random", return_value=0.85):  # > 0.7 = timeout
            with patch("parsertang.config.settings.leg3_timeout_seconds", 0.01):
                result = await simulator.simulate_sell_order(
                    exchange="kucoin",
                    symbol="BTC/USDT",
                    price=51000.0,
                    amount=0.1,
                    order_type="limit",
                    cycle_id="test-cycle-007",
                )

        assert result.success is False
        assert result.order_info.status == "timeout"

    @pytest.mark.asyncio
    async def test_market_sell_with_slippage(self, simulator):
        """Test market sell order with slippage."""
        with patch("random.random", return_value=0.5):  # success
            with patch("random.uniform", return_value=-0.1):  # -0.1% slippage
                result = await simulator.simulate_sell_order(
                    exchange="kucoin",
                    symbol="BTC/USDT",
                    price=50000.0,
                    amount=0.1,
                    order_type="market",
                    cycle_id="test-cycle-008",
                )

        assert result.success is True
        # Slippage should make filled_price slightly less than limit price
        assert result.filled_price <= 50000.0
        assert result.order_info.status == "closed"
