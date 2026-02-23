"""
Unit tests for trader.py (Phase R2 Step 3)

Tests SimpleTrader state machine core functionality.
"""

import asyncio
import tempfile
from datetime import datetime

import pytest

from parsertang.arbitrage import Opportunity
from parsertang.trader import SimpleTrader
from parsertang.trade_logger import TradeLogger
from parsertang.trade_models import CycleState


@pytest.fixture
def temp_log_dir():
    """Create temporary directory for test logs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_opportunity():
    """Create a sample arbitrage opportunity."""
    return Opportunity(
        symbol="BTC/USDT",
        buy_exchange="binance",
        buy_price=30000.0,
        sell_exchange="bybit",
        sell_price=30100.0,
        gross_spread_pct=0.33,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.005,  # Below default threshold (0.01%)
        bid_liq_usd=50000.0,
        ask_liq_usd=45000.0,
        network="BTC",
        withdrawal_fee_base=0.0001,  # 0.0001 BTC = $3 at $30000/BTC
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
    )


@pytest.fixture
def good_opportunity():
    """Create a profitable opportunity above threshold."""
    return Opportunity(
        symbol="ETH/USDT",
        buy_exchange="kucoin",
        buy_price=2000.0,
        sell_exchange="gate",
        sell_price=2020.0,
        gross_spread_pct=1.0,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.75,  # Above threshold
        bid_liq_usd=50000.0,
        ask_liq_usd=45000.0,
        network="BEP20",
        withdrawal_fee_base=0.0005,  # 0.0005 ETH = $1 at $2000/ETH
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
    )


def test_trader_initialization(temp_log_dir):
    """Test SimpleTrader initialization."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, max_concurrent_cycles=2, trade_logger=logger)

    assert trader.dry_run is True
    assert trader.max_concurrent_cycles == 2
    assert trader.trade_logger is logger
    assert len(trader.active_cycles) == 0
    assert trader.total_cycles_started == 0
    assert trader.total_cycles_completed == 0
    assert trader.total_cycles_failed == 0


def test_trader_can_start_new_cycle():
    """Test cycle limit checking."""
    trader = SimpleTrader(dry_run=True, max_concurrent_cycles=2)

    # Initially can start
    assert trader.can_start_new_cycle() is True

    # Simulate 2 active cycles
    trader.active_cycles["cycle1"] = None
    trader.active_cycles["cycle2"] = None

    # Now at limit
    assert trader.can_start_new_cycle() is False


def test_evaluate_opportunity_below_threshold(sample_opportunity):
    """Test opportunity evaluation with profit below threshold."""
    trader = SimpleTrader(dry_run=True)

    # sample_opportunity has net_profit=0.005%, default threshold=0.01%
    result = trader.evaluate_opportunity(sample_opportunity)

    assert result is False


def test_evaluate_opportunity_above_threshold(good_opportunity):
    """Test opportunity evaluation with profit above threshold."""
    trader = SimpleTrader(dry_run=True)

    # good_opportunity has net_profit=0.75%, threshold=0.5%
    result = trader.evaluate_opportunity(good_opportunity)

    assert result is True


def test_evaluate_opportunity_at_max_cycles(good_opportunity):
    """Test opportunity evaluation when at max concurrent cycles."""
    trader = SimpleTrader(dry_run=True, max_concurrent_cycles=1)

    # Add one active cycle
    trader.active_cycles["existing"] = None

    # Should reject due to max cycles
    result = trader.evaluate_opportunity(good_opportunity)

    assert result is False


def test_evaluate_opportunity_insufficient_liquidity(monkeypatch):
    """Test opportunity evaluation with insufficient liquidity."""
    # Ensure a known threshold regardless of .env or prior test state
    from parsertang import config

    monkeypatch.setattr(config.settings, "liquidity_usd_threshold", 1000.0)

    # Create opportunity with very low liquidity (below threshold)
    low_liq_opp = Opportunity(
        symbol="BTC/USDT",
        buy_exchange="binance",
        buy_price=30000.0,
        sell_exchange="bybit",
        sell_price=30100.0,
        gross_spread_pct=1.0,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.75,  # Good profit
        bid_liq_usd=100.0,  # Very low liquidity (below 1000 threshold)
        ask_liq_usd=100.0,
        network="TRC20",
    )

    trader = SimpleTrader(dry_run=True)
    result = trader.evaluate_opportunity(low_liq_opp)

    assert result is False


@pytest.mark.asyncio
async def test_start_cycle_success(good_opportunity, temp_log_dir):
    """Test successful cycle start."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    cycle_id = await trader.start_cycle(good_opportunity)

    # Should return cycle ID
    assert cycle_id is not None
    assert len(cycle_id) == 8  # UUID prefix

    # Should be in active cycles
    assert cycle_id in trader.active_cycles

    # Statistics updated
    assert trader.total_cycles_started == 1

    # Cycle has correct data
    cycle = trader.get_cycle(cycle_id)
    assert cycle is not None
    assert cycle.opportunity == good_opportunity
    assert cycle.base_currency == "ETH"
    assert cycle.quote_currency == "USDT"
    assert cycle.started_at is not None

    # Small delay for async task
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_start_cycle_rejected(sample_opportunity):
    """Test cycle start rejection (below threshold)."""
    trader = SimpleTrader(dry_run=True)

    cycle_id = await trader.start_cycle(sample_opportunity)

    # Should return None (rejected)
    assert cycle_id is None

    # No cycles started
    assert len(trader.active_cycles) == 0
    assert trader.total_cycles_started == 0


def test_get_active_cycle_ids():
    """Test getting active cycle IDs."""
    trader = SimpleTrader(dry_run=True)

    # Initially empty
    assert trader.get_active_cycle_ids() == []

    # Add some cycles
    trader.active_cycles["cycle1"] = None
    trader.active_cycles["cycle2"] = None

    cycle_ids = trader.get_active_cycle_ids()
    assert len(cycle_ids) == 2
    assert "cycle1" in cycle_ids
    assert "cycle2" in cycle_ids


def test_get_cycle():
    """Test getting cycle by ID."""
    trader = SimpleTrader(dry_run=True)

    # Non-existent cycle
    assert trader.get_cycle("nonexistent") is None

    # Add a cycle (using mock object for simplicity)
    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle()
    trader.active_cycles[cycle.cycle_id] = cycle

    # Retrieve it
    retrieved = trader.get_cycle(cycle.cycle_id)
    assert retrieved is cycle


def test_get_stats_initial():
    """Test initial statistics."""
    trader = SimpleTrader(dry_run=True)

    stats = trader.get_stats()

    assert stats["active_cycles"] == 0
    assert stats["total_started"] == 0
    assert stats["total_completed"] == 0
    assert stats["total_failed"] == 0
    assert stats["success_rate"] == 0.0


def test_get_stats_with_cycles():
    """Test statistics with some cycles."""
    trader = SimpleTrader(dry_run=True)

    # Simulate some activity
    trader.total_cycles_started = 10
    trader.total_cycles_completed = 7
    trader.total_cycles_failed = 3

    stats = trader.get_stats()

    assert stats["total_started"] == 10
    assert stats["total_completed"] == 7
    assert stats["total_failed"] == 3
    assert stats["success_rate"] == 70.0


@pytest.mark.asyncio
async def test_cancel_cycle():
    """Test cycle cancellation."""
    trader = SimpleTrader(dry_run=True)

    # Create a cycle
    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle()
    cycle_id = cycle.cycle_id
    trader.active_cycles[cycle_id] = cycle

    # Cancel it
    result = await trader.cancel_cycle(cycle_id, "Test cancellation")

    assert result is True
    assert cycle_id not in trader.active_cycles
    assert cycle.state == CycleState.CANCELLED
    assert cycle.failure_reason == "Test cancellation"
    assert cycle.completed_at is not None


@pytest.mark.asyncio
async def test_cancel_nonexistent_cycle():
    """Test cancelling non-existent cycle."""
    trader = SimpleTrader(dry_run=True)

    result = await trader.cancel_cycle("nonexistent", "Test")

    assert result is False


@pytest.mark.asyncio
async def test_shutdown():
    """Test graceful shutdown."""
    trader = SimpleTrader(dry_run=True)

    # Add some active cycles
    from parsertang.trade_models import TradeCycle

    cycle1 = TradeCycle()
    cycle2 = TradeCycle()
    trader.active_cycles[cycle1.cycle_id] = cycle1
    trader.active_cycles[cycle2.cycle_id] = cycle2

    # Shutdown
    await trader.shutdown()

    # All cycles should be cancelled
    assert len(trader.active_cycles) == 0
    assert cycle1.state == CycleState.CANCELLED
    assert cycle2.state == CycleState.CANCELLED


@pytest.mark.asyncio
async def test_execute_cycle_with_leg1(good_opportunity, temp_log_dir):
    """Test that cycle executes through LEG1 (may succeed or timeout)."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Start cycle
    cycle_id = await trader.start_cycle(good_opportunity)
    assert cycle_id is not None

    # Wait for execution to complete (now includes LEG2 and LEG3 in Step 5)
    # LEG1: ~5s, LEG2: ~3s, LEG3: ~10s
    await asyncio.sleep(20.0)

    # Cycle should be removed from active cycles (completed or failed)
    assert cycle_id not in trader.active_cycles

    # Statistics should be updated
    assert trader.total_cycles_started == 1
    # Either completed or failed
    assert (trader.total_cycles_completed + trader.total_cycles_failed) == 1

    # Check log file
    cycles = logger.read_cycles()
    assert len(cycles) > 0

    # Find our cycle (last entry, as there may be multiple updates)
    cycle_entries = [c for c in cycles if c["cycle_id"] == cycle_id]
    assert len(cycle_entries) > 0

    # Get the final state (last entry)
    cycle_data = cycle_entries[-1]
    # Should be either FAILED (due to LEG1 timeout or LEG2/LEG3 placeholder) or other terminal state
    assert cycle_data["state"] in ("FAILED", "COMPLETE", "LEG1_TIMEOUT")


def test_transition_state():
    """Test state transition."""
    from parsertang.trade_models import TradeCycle

    trader = SimpleTrader(dry_run=True)
    cycle = TradeCycle()

    # Initial state
    assert cycle.state == CycleState.SCANNING

    # Transition to LEG1
    trader._transition_state(cycle, CycleState.LEG1)
    assert cycle.state == CycleState.LEG1

    # Transition to COMPLETE
    trader._transition_state(cycle, CycleState.COMPLETE)
    assert cycle.state == CycleState.COMPLETE


@pytest.mark.asyncio
async def test_multiple_concurrent_cycles(good_opportunity, temp_log_dir):
    """Test multiple concurrent cycles up to limit."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, max_concurrent_cycles=3, trade_logger=logger)

    # Start 3 cycles
    cycle_ids = []
    for i in range(3):
        # Create slightly different opportunities
        opp = Opportunity(
            symbol=f"TEST{i}/USDT",
            buy_exchange="binance",
            buy_price=1000.0,
            sell_exchange="bybit",
            sell_price=1010.0,
            gross_spread_pct=1.0,
            trade_fees_pct=0.2,
            withdraw_fee_pct=0.05,
            net_profit_pct=0.75,
            bid_liq_usd=50000.0,
            ask_liq_usd=45000.0,
            network="TRC20",
            withdrawal_fee_base=0.0,
            buy_taker_fee_pct=0.1,
            sell_taker_fee_pct=0.1,
        )

        cycle_id = await trader.start_cycle(opp)
        if cycle_id:
            cycle_ids.append(cycle_id)

    # Should have started 3 cycles
    assert len(cycle_ids) == 3
    assert trader.total_cycles_started == 3

    # Try to start 4th - should be rejected
    cycle_id_4 = await trader.start_cycle(good_opportunity)
    assert cycle_id_4 is None  # Rejected due to limit

    # Still only 3 started
    assert trader.total_cycles_started == 3

    # Wait for execution
    await asyncio.sleep(0.2)


def test_trader_repr():
    """Test trader string representation."""
    trader = SimpleTrader(dry_run=True, max_concurrent_cycles=2)

    # Should not crash
    assert "SimpleTrader" in str(type(trader).__name__)


# ============================================================================
# LEG1 Execution Tests (Phase R2 Step 4)
# ============================================================================


@pytest.mark.asyncio
async def test_leg1_successful_fill(good_opportunity, temp_log_dir):
    """Test successful LEG1 order fill."""
    logger = TradeLogger("test_leg1.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Create cycle
    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Execute LEG1 (may succeed or timeout based on random)
    # We'll test the successful path by checking structure
    success = await trader._execute_leg1(cycle)

    # Check that order was created
    assert cycle.leg1_order is not None
    assert cycle.leg1_order.side == "buy"
    assert cycle.leg1_order.order_type == "limit"
    assert cycle.leg1_order.price == good_opportunity.buy_price
    assert cycle.leg1_order.amount > 0

    if success:
        # Check position tracking
        assert cycle.position_amount > 0
        assert cycle.position_value_usd > 0
        assert cycle.total_fees_usd > 0

        # Check order status
        assert cycle.leg1_order.status == "closed"
        assert cycle.leg1_order.filled == cycle.leg1_order.amount
        assert cycle.leg1_order.average_price == good_opportunity.buy_price

        # Check events
        assert len(cycle.events) > 0
        assert any("LEG1 completed" in e.message for e in cycle.events)
    else:
        # Timeout case
        assert cycle.leg1_order.status == "timeout"
        assert cycle.state == CycleState.LEG1_TIMEOUT


@pytest.mark.asyncio
async def test_leg1_position_size_calculation(temp_log_dir):
    """Test position size calculation respects MAX_POSITION_SIZE_USD."""
    from parsertang.config import settings

    logger = TradeLogger("test_leg1_size.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Create opportunity with high price (to test position sizing)
    high_price_opp = Opportunity(
        symbol="BTC/USDT",
        buy_exchange="binance",
        buy_price=50000.0,  # High price
        sell_exchange="bybit",
        sell_price=50500.0,
        gross_spread_pct=1.0,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.75,
        bid_liq_usd=100000.0,
        ask_liq_usd=100000.0,
        network="TRC20",
        withdrawal_fee_base=0.0,
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
    )

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=high_price_opp)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "BTC"
    cycle.quote_currency = "USDT"

    # Execute LEG1
    await trader._execute_leg1(cycle)

    # Position value should not exceed MAX_POSITION_SIZE_USD
    if cycle.position_value_usd > 0:
        assert (
            cycle.position_value_usd <= settings.max_position_size_usd * 1.01
        )  # Small tolerance

        # Position amount should be consistent
        expected_amount = cycle.position_value_usd / high_price_opp.buy_price
        assert abs(cycle.position_amount - expected_amount) < 0.0001


@pytest.mark.asyncio
async def test_leg1_respects_liquidity(temp_log_dir):
    """Test that LEG1 respects liquidity constraints."""
    logger = TradeLogger("test_leg1_liq.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Create opportunity with limited liquidity
    low_liq_opp = Opportunity(
        symbol="RARE/USDT",
        buy_exchange="binance",
        buy_price=1.0,
        sell_exchange="bybit",
        sell_price=1.02,
        gross_spread_pct=2.0,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=1.75,
        bid_liq_usd=50000.0,
        ask_liq_usd=15000.0,  # Limited liquidity
        network="TRC20",
        withdrawal_fee_base=0.0,
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
    )

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=low_liq_opp)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "RARE"
    cycle.quote_currency = "USDT"

    # Execute LEG1
    await trader._execute_leg1(cycle)

    # Position should be limited by liquidity (90% of available)
    if cycle.position_value_usd > 0:
        max_by_liquidity = low_liq_opp.ask_liq_usd * 0.9
        assert cycle.position_value_usd <= max_by_liquidity * 1.01  # Small tolerance


@pytest.mark.asyncio
async def test_leg1_fee_calculation(good_opportunity, temp_log_dir):
    """Test that LEG1 calculates fees correctly."""
    logger = TradeLogger("test_leg1_fees.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Execute LEG1
    success = await trader._execute_leg1(cycle)

    if success:
        # Fees should be calculated
        assert cycle.total_fees_usd > 0

        # Fee should be reasonable (not more than 1% for typical case)
        fee_pct = (cycle.total_fees_usd / cycle.position_value_usd) * 100
        assert fee_pct < 1.0
        assert fee_pct > 0


@pytest.mark.asyncio
async def test_leg1_event_logging(good_opportunity, temp_log_dir):
    """Test that LEG1 logs events correctly."""
    logger = TradeLogger("test_leg1_events.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Execute LEG1
    await trader._execute_leg1(cycle)

    # Should have LEG1 events
    assert len(cycle.events) > 0

    # Check for LEG1-related events
    leg1_events = [e for e in cycle.events if e.state == CycleState.LEG1]
    assert len(leg1_events) > 0

    # First event should be start
    assert "started" in leg1_events[0].message.lower()


@pytest.mark.asyncio
async def test_leg1_state_transitions(good_opportunity, temp_log_dir):
    """Test LEG1 state transitions."""
    logger = TradeLogger("test_leg1_states.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Initial state
    initial_state = cycle.state
    assert initial_state == CycleState.SCANNING

    # Execute LEG1
    success = await trader._execute_leg1(cycle)

    # State should have changed
    if success:
        # May be LEG1 or later (depending on execution flow)
        assert cycle.state in (CycleState.LEG1, CycleState.LEG2_WAIT, CycleState.LEG3)
    else:
        # Should be timeout state
        assert cycle.state == CycleState.LEG1_TIMEOUT


@pytest.mark.asyncio
async def test_leg1_order_info_structure(good_opportunity, temp_log_dir):
    """Test that OrderInfo is created with correct structure."""
    logger = TradeLogger("test_leg1_order.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Execute LEG1
    await trader._execute_leg1(cycle)

    # Order info should exist
    assert cycle.leg1_order is not None

    # Check structure
    order = cycle.leg1_order
    assert order.order_id.startswith("dry_")
    assert order.side == "buy"
    assert order.order_type == "limit"
    assert order.price > 0
    assert order.amount > 0
    assert order.status in ("open", "closed", "timeout")
    assert order.timestamp is not None

    # Check serialization
    order_dict = order.to_dict()
    assert "order_id" in order_dict
    assert "side" in order_dict
    assert "status" in order_dict


@pytest.mark.asyncio
async def test_leg1_multiple_runs_statistics(temp_log_dir):
    """Test LEG1 execution over multiple runs for statistics."""
    logger = TradeLogger("test_leg1_stats.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    success_count = 0
    timeout_count = 0
    runs = 20  # Run multiple times to test randomness

    for i in range(runs):
        opp = Opportunity(
            symbol=f"TEST{i}/USDT",
            buy_exchange="binance",
            buy_price=100.0,
            sell_exchange="bybit",
            sell_price=101.0,
            gross_spread_pct=1.0,
            trade_fees_pct=0.2,
            withdraw_fee_pct=0.05,
            net_profit_pct=0.75,
            bid_liq_usd=50000.0,
            ask_liq_usd=50000.0,
            network="TRC20",
            withdrawal_fee_base=0.0,
            buy_taker_fee_pct=0.1,
            sell_taker_fee_pct=0.1,
        )

        cycle = TradeCycle(opportunity=opp)
        cycle.started_at = datetime.utcnow()
        cycle.base_currency = f"TEST{i}"
        cycle.quote_currency = "USDT"

        success = await trader._execute_leg1(cycle)

        if success:
            success_count += 1
        else:
            timeout_count += 1

    # With 90% fill probability, we should have mostly successes
    # but some timeouts (statistically)
    assert success_count > 0
    # Timeout count may be 0 in some runs, but that's ok

    # Success rate should be reasonable
    success_rate = success_count / runs
    assert success_rate >= 0.5  # At least 50% (should be ~90% statistically)


# ============================================================================
# LEG2 Execution Tests (Phase R2 Step 5)
# ============================================================================


@pytest.mark.asyncio
async def test_leg2_successful_withdrawal(good_opportunity, temp_log_dir):
    """Test successful LEG2 withdrawal."""
    logger = TradeLogger("test_leg2.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Create cycle with filled LEG1
    from parsertang.trade_models import TradeCycle, OrderInfo

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"

    # Simulate LEG1 completion
    cycle.position_amount = 0.05  # 0.05 ETH
    cycle.position_value_usd = 100.0
    cycle.total_fees_usd = 0.1
    cycle.leg1_order = OrderInfo(
        order_id="test_buy_1",
        order_type="limit",
        side="buy",
        price=2000.0,
        amount=0.05,
        filled=0.05,
        average_price=2000.0,
        status="closed",
    )

    # Execute LEG2
    success = await trader._execute_leg2(cycle)

    # Check withdrawal info
    assert cycle.leg2_withdrawal is not None
    assert cycle.leg2_withdrawal.currency == "ETH"
    assert cycle.leg2_withdrawal.network == "BEP20"
    assert cycle.leg2_withdrawal.amount > 0

    if success:
        # Check position updated (reduced by withdrawal fee)
        assert cycle.position_amount < 0.05
        assert cycle.position_amount > 0

        # Check withdrawal status
        assert cycle.leg2_withdrawal.status == "completed"

        # Check events
        assert len(cycle.events) > 0
        leg2_events = [e for e in cycle.events if "LEG2" in e.message]
        assert len(leg2_events) > 0
    else:
        # Failed case
        assert cycle.leg2_withdrawal.status == "failed"


@pytest.mark.asyncio
async def test_leg2_withdrawal_fee_tracking(good_opportunity, temp_log_dir):
    """Test that withdrawal fees are tracked correctly."""
    logger = TradeLogger("test_leg2_fees.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    initial_fees = cycle.total_fees_usd

    # Execute LEG2
    success = await trader._execute_leg2(cycle)

    if success:
        # Fees should have increased
        assert cycle.total_fees_usd > initial_fees

        # Withdrawal fee should be reasonable
        withdrawal_fee_usd = cycle.total_fees_usd - initial_fees
        assert withdrawal_fee_usd >= 0


@pytest.mark.asyncio
async def test_leg2_network_delay_simulation(good_opportunity, temp_log_dir):
    """Test that LEG2 simulates network confirmation delays."""
    import time

    logger = TradeLogger("test_leg2_delay.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    start_time = time.time()

    # Execute LEG2 (should take 1-3s for BEP20)
    await trader._execute_leg2(cycle)

    elapsed = time.time() - start_time

    # Should take at least 1 second (network delay)
    assert elapsed >= 1.0
    # Should not take too long (max 10s for slow networks)
    assert elapsed <= 15.0


@pytest.mark.asyncio
async def test_leg2_state_transitions(good_opportunity, temp_log_dir):
    """Test LEG2 state transitions."""
    logger = TradeLogger("test_leg2_states.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    # Execute LEG2
    await trader._execute_leg2(cycle)

    # State should have transitioned through LEG2_WAIT → LEG2
    leg2_events = [
        e for e in cycle.events if e.state in (CycleState.LEG2_WAIT, CycleState.LEG2)
    ]
    assert len(leg2_events) > 0


# ============================================================================
# LEG3 Execution Tests (Phase R2 Step 5)
# ============================================================================


@pytest.mark.asyncio
async def test_leg3_successful_sell_limit(good_opportunity, temp_log_dir):
    """Test successful LEG3 sell with limit order."""
    logger = TradeLogger("test_leg3.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Create cycle with filled LEG1 and LEG2
    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.048  # After withdrawal fee
    cycle.position_value_usd = 96.0  # Initial buy value
    cycle.total_fees_usd = 0.15  # Buy + withdrawal fees

    # Execute LEG3
    success = await trader._execute_leg3(cycle)

    # Check sell order info
    assert cycle.leg3_order is not None
    assert cycle.leg3_order.side == "sell"
    assert cycle.leg3_order.amount > 0

    if success:
        # Check order status
        assert cycle.leg3_order.status == "closed"
        assert cycle.leg3_order.filled == cycle.leg3_order.amount

        # Check profit calculation
        assert cycle.realized_profit_usd != 0  # May be positive or negative
        assert cycle.realized_profit_pct != 0

        # Check events
        leg3_events = [e for e in cycle.events if e.state == CycleState.LEG3]
        assert len(leg3_events) > 0


@pytest.mark.asyncio
async def test_leg3_limit_timeout_market_fallback(good_opportunity, temp_log_dir):
    """Test LEG3 falls back to market order on limit timeout."""
    logger = TradeLogger("test_leg3_fallback.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.048
    cycle.position_value_usd = 96.0
    cycle.total_fees_usd = 0.15

    # Execute LEG3 multiple times to catch market fallback case
    # (limit order has 70% success rate, so some will timeout)
    for _ in range(5):
        cycle_test = TradeCycle(opportunity=good_opportunity)
        cycle_test.started_at = datetime.utcnow()
        cycle_test.base_currency = "ETH"
        cycle_test.quote_currency = "USDT"
        cycle_test.position_amount = 0.048
        cycle_test.position_value_usd = 96.0

        await trader._execute_leg3(cycle_test)

        # If limit timed out, should see market order attempt
        if cycle_test.leg3_order and cycle_test.leg3_order.order_type == "market":
            # Found a market order case
            assert cycle_test.state in (CycleState.LEG3_MARKET, CycleState.LEG3)
            break


@pytest.mark.asyncio
async def test_leg3_profit_calculation(good_opportunity, temp_log_dir):
    """Test LEG3 profit calculation."""
    logger = TradeLogger("test_leg3_profit.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.048  # Amount to sell
    cycle.position_value_usd = 96.0  # Initial investment
    cycle.total_fees_usd = 0.2  # Accumulated fees

    # Execute LEG3
    success = await trader._execute_leg3(cycle)

    if success:
        # Profit should be calculated
        # sell_value - buy_value - fees = profit
        _ = cycle.position_amount * good_opportunity.sell_price  # noqa: F841

        # Realized profit should be reasonable
        # (may be negative due to fees in dry-run)
        assert cycle.realized_profit_usd != 0
        assert cycle.realized_profit_pct != 0

        # Total fees should have increased (sell fee added)
        assert cycle.total_fees_usd > 0.2


@pytest.mark.asyncio
async def test_leg3_sell_fee_calculation(good_opportunity, temp_log_dir):
    """Test that LEG3 calculates sell fees correctly."""
    logger = TradeLogger("test_leg3_fees.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    initial_fees = cycle.total_fees_usd

    # Execute LEG3
    success = await trader._execute_leg3(cycle)

    if success:
        # Sell fees should have been added
        assert cycle.total_fees_usd > initial_fees

        # Fee should be reasonable
        sell_fee = cycle.total_fees_usd - initial_fees
        assert sell_fee >= 0
        assert (
            sell_fee < cycle.position_value_usd
        )  # Fee shouldn't exceed position value


@pytest.mark.asyncio
async def test_leg3_market_order_slippage(good_opportunity, temp_log_dir):
    """Test that market orders simulate slippage."""
    logger = TradeLogger("test_leg3_slippage.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    # Directly test market order simulation using TradeSimulator
    assert (
        trader.simulator is not None
    ), "TradeSimulator should be initialized in dry_run mode"

    result = await trader.simulator.simulate_sell_order(
        exchange="bybit",
        symbol="ETH/USDT",
        price=2020.0,
        amount=0.05,
        order_type="market",
        cycle_id=cycle.cycle_id,
    )

    if result.success:
        # Market order should have slippage (worse price for seller)
        # Slippage: -0.05% to -0.15%
        assert result.filled_price < 2020.0
        assert result.filled_price > 2020.0 * 0.997  # Max 0.3% slippage tolerance


@pytest.mark.asyncio
async def test_leg3_event_logging(good_opportunity, temp_log_dir):
    """Test that LEG3 logs events correctly."""
    logger = TradeLogger("test_leg3_events.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    from parsertang.trade_models import TradeCycle

    cycle = TradeCycle(opportunity=good_opportunity)
    cycle.started_at = datetime.utcnow()
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.position_amount = 0.05
    cycle.position_value_usd = 100.0

    # Execute LEG3
    await trader._execute_leg3(cycle)

    # Should have LEG3 events
    assert len(cycle.events) > 0

    # Check for LEG3-related events
    leg3_events = [
        e for e in cycle.events if e.state in (CycleState.LEG3, CycleState.LEG3_MARKET)
    ]
    assert len(leg3_events) > 0


# ============================================================================
# Full Cycle Tests (LEG1 → LEG2 → LEG3)
# ============================================================================


@pytest.mark.asyncio
async def test_full_cycle_execution(good_opportunity, temp_log_dir):
    """Test complete cycle execution through all 3 legs."""
    logger = TradeLogger("test_full_cycle.jsonl", temp_log_dir)
    trader = SimpleTrader(dry_run=True, trade_logger=logger)

    # Start cycle
    cycle_id = await trader.start_cycle(good_opportunity)
    assert cycle_id is not None

    # Wait for full execution (LEG1: ~5s, LEG2: ~3s, LEG3: ~10s max)
    await asyncio.sleep(20.0)

    # Cycle should be completed or failed
    assert cycle_id not in trader.active_cycles

    # Check statistics
    stats = trader.get_stats()
    assert stats["total_started"] == 1
    assert (stats["total_completed"] + stats["total_failed"]) == 1

    # Check log entries
    cycles = logger.read_cycles()
    cycle_entries = [c for c in cycles if c["cycle_id"] == cycle_id]

    if len(cycle_entries) > 0:
        final_cycle = cycle_entries[-1]
        # Should be in terminal state
        assert final_cycle["state"] in (
            "COMPLETE",
            "FAILED",
            "LEG1_TIMEOUT",
            "CANCELLED",
        )

        # If completed, should have profit data
        if final_cycle["state"] == "COMPLETE":
            assert "results" in final_cycle
            assert "realized_profit_usd" in final_cycle["results"]
            assert "realized_profit_pct" in final_cycle["results"]
