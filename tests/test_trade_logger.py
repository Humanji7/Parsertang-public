"""
Unit tests for trade_logger.py (Phase R2)

Tests the JSONL logging functionality for trade cycles.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from parsertang.arbitrage import Opportunity
from parsertang.trade_logger import TradeLogger, get_default_logger, log_cycle
from parsertang.trade_models import CycleState, OrderInfo, TradeCycle, WithdrawalInfo


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_cycle():
    """Create a sample trade cycle for testing."""
    opp = Opportunity(
        symbol="BTC/USDT",
        buy_exchange="binance",
        buy_price=30000.0,
        sell_exchange="bybit",
        sell_price=30100.0,
        gross_spread_pct=0.33,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.08,
        bid_liq_usd=50000.0,
        ask_liq_usd=45000.0,
        network="TRC20",
    )

    cycle = TradeCycle(opportunity=opp)
    cycle.base_currency = "BTC"
    cycle.quote_currency = "USDT"
    cycle.state = CycleState.COMPLETE
    cycle.started_at = datetime.utcnow()
    cycle.completed_at = datetime.utcnow()
    cycle.realized_profit_usd = 8.0
    cycle.realized_profit_pct = 0.08

    # Add some events
    cycle.log_event(CycleState.LEG1, "Buy order filled")
    cycle.log_event(CycleState.LEG2, "Withdrawal completed")
    cycle.log_event(CycleState.LEG3, "Sell order filled")
    cycle.log_event(CycleState.COMPLETE, "Cycle completed successfully")

    return cycle


def test_trade_logger_initialization(temp_log_dir):
    """Test TradeLogger initialization."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    assert logger.log_path.exists()
    assert logger.log_path.name == "test_log.jsonl"
    assert logger.log_path.parent == Path(temp_log_dir)


def test_log_cycle(temp_log_dir, sample_cycle):
    """Test logging a single cycle."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Log the cycle
    logger.log_cycle(sample_cycle)

    # Read the file
    with open(logger.log_path, "r") as f:
        lines = f.readlines()

    assert len(lines) == 1

    # Parse JSON
    cycle_dict = json.loads(lines[0])
    assert cycle_dict["cycle_id"] == sample_cycle.cycle_id
    assert cycle_dict["state"] == "COMPLETE"
    assert cycle_dict["opportunity"]["symbol"] == "BTC/USDT"
    assert len(cycle_dict["events"]) == 4


def test_log_multiple_cycles(temp_log_dir):
    """Test logging multiple cycles."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Create and log 5 cycles
    for i in range(5):
        cycle = TradeCycle()
        cycle.log_event(CycleState.SCANNING, f"Cycle {i}")
        logger.log_cycle(cycle)

    # Count cycles
    assert logger.count_cycles() == 5


def test_log_cycle_update(temp_log_dir, sample_cycle):
    """Test logging cycle updates."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Log initial state
    logger.log_cycle(sample_cycle)

    # Log update
    sample_cycle.state = CycleState.LEG1
    logger.log_cycle_update(sample_cycle, "LEG1 started")

    # Should have 2 lines
    assert logger.count_cycles() == 2

    # Read both entries
    cycles = logger.read_cycles()
    assert len(cycles) == 2
    assert cycles[1]["_event"] == "LEG1 started"


def test_count_cycles(temp_log_dir):
    """Test counting cycles in log file."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Empty log
    assert logger.count_cycles() == 0

    # Add cycles
    for i in range(3):
        cycle = TradeCycle()
        logger.log_cycle(cycle)

    assert logger.count_cycles() == 3


def test_read_cycles(temp_log_dir, sample_cycle):
    """Test reading cycles from log file."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Log multiple cycles
    cycle_ids = []
    for i in range(3):
        cycle = TradeCycle()
        cycle.log_event(CycleState.SCANNING, f"Test {i}")
        logger.log_cycle(cycle)
        cycle_ids.append(cycle.cycle_id)

    # Read all cycles
    cycles = logger.read_cycles()
    assert len(cycles) == 3

    # Verify cycle IDs
    read_ids = [c["cycle_id"] for c in cycles]
    assert read_ids == cycle_ids


def test_read_cycles_with_limit(temp_log_dir):
    """Test reading cycles with limit."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Log 5 cycles
    for i in range(5):
        cycle = TradeCycle()
        logger.log_cycle(cycle)

    # Read with limit
    cycles = logger.read_cycles(limit=2)
    assert len(cycles) == 2


def test_get_stats(temp_log_dir):
    """Test getting cycle statistics."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Log cycles with different states
    # 2 completed
    for i in range(2):
        cycle = TradeCycle()
        cycle.state = CycleState.COMPLETE
        cycle.realized_profit_usd = 10.0
        logger.log_cycle(cycle)

    # 1 failed
    cycle = TradeCycle()
    cycle.state = CycleState.FAILED
    logger.log_cycle(cycle)

    # 1 scanning
    cycle = TradeCycle()
    cycle.state = CycleState.SCANNING
    logger.log_cycle(cycle)

    # Get stats
    stats = logger.get_stats()

    assert stats["total_cycles"] == 4
    assert stats["successful_cycles"] == 2
    assert stats["failed_cycles"] == 1
    assert stats["total_profit_usd"] == 20.0
    assert stats["by_state"]["COMPLETE"] == 2
    assert stats["by_state"]["FAILED"] == 1
    assert stats["by_state"]["SCANNING"] == 1


def test_rotate_log(temp_log_dir):
    """Test log rotation."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Write some data
    for i in range(5):
        cycle = TradeCycle()
        logger.log_cycle(cycle)

    # Force rotation (with very small size)
    rotated = logger.rotate_log(max_size_mb=0.0001)  # Very small threshold

    assert rotated is True

    # Check rotated file exists
    rotated_path = Path(f"{logger.log_path}.1")
    assert rotated_path.exists()

    # New log file should be empty
    assert logger.count_cycles() == 0


def test_singleton_get_instance(temp_log_dir):
    """Test singleton pattern for get_instance."""
    logger1 = TradeLogger.get_instance("test_log.jsonl", temp_log_dir)
    logger2 = TradeLogger.get_instance("test_log.jsonl", temp_log_dir)

    # Should be the same instance
    assert logger1 is logger2


def test_different_files_different_instances(temp_log_dir):
    """Test that different log files get different instances."""
    logger1 = TradeLogger.get_instance("log1.jsonl", temp_log_dir)
    logger2 = TradeLogger.get_instance("log2.jsonl", temp_log_dir)

    # Should be different instances
    assert logger1 is not logger2


def test_thread_safety_simulation(temp_log_dir):
    """Test thread safety by logging multiple cycles rapidly."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Simulate rapid logging (like concurrent threads would)
    cycles = []
    for i in range(20):
        cycle = TradeCycle()
        cycle.log_event(CycleState.SCANNING, f"Rapid {i}")
        cycles.append(cycle)
        logger.log_cycle(cycle)

    # All cycles should be logged
    assert logger.count_cycles() == 20

    # Verify all cycles are readable
    read_cycles = logger.read_cycles()
    assert len(read_cycles) == 20


def test_malformed_json_handling(temp_log_dir):
    """Test handling of malformed JSON in log file."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Write valid cycle
    cycle = TradeCycle()
    logger.log_cycle(cycle)

    # Manually add malformed line
    with open(logger.log_path, "a") as f:
        f.write("This is not JSON\n")
        f.write("{broken json\n")

    # Write another valid cycle
    cycle2 = TradeCycle()
    logger.log_cycle(cycle2)

    # Should count all lines
    assert logger.count_cycles() == 4

    # But only parse valid JSON (should skip malformed lines)
    cycles = logger.read_cycles()
    assert len(cycles) == 2  # Only 2 valid JSON objects


def test_empty_log_file(temp_log_dir):
    """Test operations on empty log file."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    assert logger.count_cycles() == 0
    assert logger.read_cycles() == []

    stats = logger.get_stats()
    assert stats["total_cycles"] == 0
    assert stats["successful_cycles"] == 0


def test_repr(temp_log_dir):
    """Test string representation."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Empty logger
    repr_str = repr(logger)
    assert "TradeLogger" in repr_str
    assert "cycles=0" in repr_str

    # Add cycles
    for i in range(3):
        cycle = TradeCycle()
        logger.log_cycle(cycle)

    repr_str = repr(logger)
    assert "cycles=3" in repr_str


def test_get_default_logger():
    """Test global default logger."""
    logger1 = get_default_logger()
    logger2 = get_default_logger()

    # Should return the same instance
    assert logger1 is logger2


def test_convenience_log_cycle_function(sample_cycle):
    """Test convenience log_cycle function."""
    # This should work without errors
    # Note: Uses default logger in cwd, so we just test it doesn't crash
    try:
        log_cycle(sample_cycle)
    except Exception as e:
        pytest.fail(f"log_cycle() raised exception: {e}")


def test_complete_cycle_serialization(temp_log_dir):
    """Test complete cycle with all legs is properly serialized."""
    logger = TradeLogger("test_log.jsonl", temp_log_dir)

    # Create complete cycle
    opp = Opportunity(
        symbol="ETH/USDT",
        buy_exchange="kucoin",
        buy_price=2000.0,
        sell_exchange="gate",
        sell_price=2010.0,
        gross_spread_pct=0.5,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.05,
        net_profit_pct=0.25,
        bid_liq_usd=30000.0,
        ask_liq_usd=28000.0,
        network="BEP20",
    )

    cycle = TradeCycle(opportunity=opp)
    cycle.state = CycleState.COMPLETE
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.started_at = datetime.utcnow()
    cycle.completed_at = datetime.utcnow()

    # Add all legs
    cycle.leg1_order = OrderInfo(
        order_id="buy_123",
        order_type="limit",
        side="buy",
        price=2000.0,
        amount=1.0,
        filled=1.0,
        average_price=2000.0,
        status="closed",
    )

    cycle.leg2_withdrawal = WithdrawalInfo(
        withdrawal_id="wd_456",
        currency="ETH",
        network="BEP20",
        amount=1.0,
        fee=0.001,
        status="completed",
    )

    cycle.leg3_order = OrderInfo(
        order_id="sell_789",
        order_type="limit",
        side="sell",
        price=2010.0,
        amount=1.0,
        filled=1.0,
        average_price=2010.0,
        status="closed",
    )

    cycle.realized_profit_usd = 10.0
    cycle.realized_profit_pct = 0.25

    # Log it
    logger.log_cycle(cycle)

    # Read and verify
    cycles = logger.read_cycles()
    assert len(cycles) == 1

    cycle_data = cycles[0]
    assert cycle_data["state"] == "COMPLETE"
    assert "leg1_order" in cycle_data
    assert "leg2_withdrawal" in cycle_data
    assert "leg3_order" in cycle_data
    assert cycle_data["leg1_order"]["order_id"] == "buy_123"
    assert cycle_data["leg2_withdrawal"]["withdrawal_id"] == "wd_456"
    assert cycle_data["leg3_order"]["order_id"] == "sell_789"
    assert cycle_data["results"]["realized_profit_usd"] == 10.0
