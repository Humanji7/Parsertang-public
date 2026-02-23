"""
Unit tests for trade_models.py (Phase R2)

Tests the data structures and state machine for 3-leg arbitrage cycles.
"""

from datetime import datetime, timedelta


from parsertang.arbitrage import Opportunity
from parsertang.trade_models import (
    CycleEvent,
    CycleState,
    OrderInfo,
    TradeCycle,
    WithdrawalInfo,
)


def test_cycle_state_enum():
    """Test CycleState enum values."""
    assert CycleState.SCANNING.value == "SCANNING"
    assert CycleState.LEG1.value == "LEG1"
    assert CycleState.LEG2_WAIT.value == "LEG2_WAIT"
    assert CycleState.LEG3.value == "LEG3"
    assert CycleState.COMPLETE.value == "COMPLETE"
    assert CycleState.FAILED.value == "FAILED"


def test_order_info_creation():
    """Test OrderInfo dataclass creation."""
    order = OrderInfo(
        order_id="order_123",
        order_type="limit",
        side="buy",
        price=1.0,
        amount=100.0,
        filled=50.0,
        average_price=1.01,
        status="closed",
    )

    assert order.order_id == "order_123"
    assert order.side == "buy"
    assert order.filled == 50.0

    # Test to_dict serialization
    data = order.to_dict()
    assert data["order_id"] == "order_123"
    assert data["order_type"] == "limit"
    assert data["filled"] == 50.0
    assert "timestamp" in data


def test_withdrawal_info_creation():
    """Test WithdrawalInfo dataclass creation."""
    withdrawal = WithdrawalInfo(
        withdrawal_id="wd_456",
        currency="USDT",
        network="TRC20",
        amount=100.0,
        fee=1.0,
        status="completed",
    )

    assert withdrawal.withdrawal_id == "wd_456"
    assert withdrawal.network == "TRC20"
    assert withdrawal.fee == 1.0

    # Test to_dict serialization
    data = withdrawal.to_dict()
    assert data["currency"] == "USDT"
    assert data["network"] == "TRC20"
    assert data["amount"] == 100.0
    assert "timestamp" in data


def test_cycle_event_creation():
    """Test CycleEvent dataclass creation."""
    event = CycleEvent(
        timestamp=datetime.utcnow(),
        state=CycleState.LEG1,
        message="Order placed",
        details={"order_id": "123"},
    )

    assert event.state == CycleState.LEG1
    assert event.message == "Order placed"
    assert event.details["order_id"] == "123"

    # Test to_dict serialization
    data = event.to_dict()
    assert data["state"] == "LEG1"
    assert data["message"] == "Order placed"
    assert data["details"]["order_id"] == "123"


def test_trade_cycle_creation():
    """Test TradeCycle creation with defaults."""
    cycle = TradeCycle()

    # Check defaults
    assert cycle.state == CycleState.SCANNING
    assert len(cycle.cycle_id) == 8  # UUID prefix
    assert cycle.opportunity is None
    assert cycle.started_at is None
    assert cycle.completed_at is None
    assert len(cycle.events) == 0


def test_trade_cycle_with_opportunity():
    """Test TradeCycle with Opportunity attached."""
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

    assert cycle.opportunity.symbol == "BTC/USDT"
    assert cycle.opportunity.buy_exchange == "binance"
    assert cycle.opportunity.sell_exchange == "bybit"
    assert cycle.base_currency == "BTC"


def test_trade_cycle_log_event():
    """Test logging events in a cycle."""
    cycle = TradeCycle()

    cycle.log_event(CycleState.SCANNING, "Evaluating opportunity")
    cycle.log_event(CycleState.LEG1, "Placing buy order", {"price": 30000})

    assert len(cycle.events) == 2
    assert cycle.events[0].state == CycleState.SCANNING
    assert cycle.events[1].message == "Placing buy order"
    assert cycle.events[1].details["price"] == 30000


def test_trade_cycle_duration():
    """Test cycle duration calculation."""
    cycle = TradeCycle()

    # No duration if not started
    assert cycle.duration_seconds() == 0.0

    # Set started time
    cycle.started_at = datetime.utcnow() - timedelta(seconds=10)
    duration = cycle.duration_seconds()
    assert 9.5 < duration < 10.5  # Allow some tolerance

    # With completion time
    cycle.completed_at = cycle.started_at + timedelta(seconds=15)
    assert cycle.duration_seconds() == 15.0


def test_trade_cycle_to_dict():
    """Test full cycle serialization to dict."""
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
    cycle.state = CycleState.LEG1
    cycle.base_currency = "ETH"
    cycle.quote_currency = "USDT"
    cycle.started_at = datetime.utcnow()

    # Add order
    cycle.leg1_order = OrderInfo(
        order_id="ord_123",
        order_type="limit",
        side="buy",
        price=2000.0,
        amount=1.0,
        filled=1.0,
        average_price=2000.0,
        status="closed",
    )

    cycle.log_event(CycleState.LEG1, "Order filled")

    # Serialize
    data = cycle.to_dict()

    # Verify structure
    assert data["cycle_id"] == cycle.cycle_id
    assert data["state"] == "LEG1"
    assert data["opportunity"]["symbol"] == "ETH/USDT"
    assert data["opportunity"]["buy_exchange"] == "kucoin"
    assert data["leg1_order"]["order_id"] == "ord_123"
    assert data["position"]["base_currency"] == "ETH"
    assert data["position"]["quote_currency"] == "USDT"
    assert len(data["events"]) == 1
    assert data["events"][0]["message"] == "Order filled"


def test_trade_cycle_repr():
    """Test human-readable representation."""
    cycle = TradeCycle()
    repr_str = repr(cycle)
    assert "TradeCycle" in repr_str
    assert "SCANNING" in repr_str

    # With opportunity
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

    cycle.opportunity = opp
    cycle.state = CycleState.COMPLETE
    cycle.realized_profit_pct = 0.08

    repr_str = repr(cycle)
    assert "BTC/USDT" in repr_str
    assert "binance→bybit" in repr_str
    assert "profit=0.08%" in repr_str


def test_trade_cycle_with_failure():
    """Test cycle with failure state."""
    cycle = TradeCycle()
    cycle.state = CycleState.FAILED
    cycle.failure_reason = "Order timeout in LEG1"

    data = cycle.to_dict()
    assert data["state"] == "FAILED"
    assert data["failure_reason"] == "Order timeout in LEG1"


def test_trade_cycle_complete_flow():
    """Test complete cycle with all legs."""
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
    cycle.started_at = datetime.utcnow()

    # LEG1: Buy order
    cycle.state = CycleState.LEG1
    cycle.leg1_order = OrderInfo(
        order_id="buy_123",
        order_type="limit",
        side="buy",
        price=30000.0,
        amount=0.1,
        filled=0.1,
        average_price=30000.0,
        status="closed",
    )
    cycle.log_event(CycleState.LEG1, "Buy order filled")

    # LEG2: Withdrawal
    cycle.state = CycleState.LEG2
    cycle.leg2_withdrawal = WithdrawalInfo(
        withdrawal_id="wd_456",
        currency="BTC",
        network="TRC20",
        amount=0.1,
        fee=0.0001,
        status="completed",
    )
    cycle.log_event(CycleState.LEG2, "Withdrawal completed")

    # LEG3: Sell order
    cycle.state = CycleState.LEG3
    cycle.leg3_order = OrderInfo(
        order_id="sell_789",
        order_type="limit",
        side="sell",
        price=30100.0,
        amount=0.1,
        filled=0.1,
        average_price=30100.0,
        status="closed",
    )
    cycle.log_event(CycleState.LEG3, "Sell order filled")

    # Complete
    cycle.state = CycleState.COMPLETE
    cycle.completed_at = datetime.utcnow()
    cycle.realized_profit_usd = 10.0
    cycle.realized_profit_pct = 0.08
    cycle.log_event(CycleState.COMPLETE, "Cycle completed successfully")

    # Verify serialization
    data = cycle.to_dict()
    assert data["state"] == "COMPLETE"
    assert "leg1_order" in data
    assert "leg2_withdrawal" in data
    assert "leg3_order" in data
    assert len(data["events"]) == 4
    assert data["results"]["realized_profit_usd"] == 10.0
