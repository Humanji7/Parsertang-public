"""
Trade cycle models for МДРК Phase R2+

This module defines the data structures for managing 3-leg arbitrage cycles:
- LEG1: Auto buy (limit order with timeout)
- LEG2: Semi-auto withdrawal (requires confirmation in R3+)
- LEG3: Auto sell (limit order → market order fallback)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from parsertang.arbitrage import Opportunity


class CycleState(str, Enum):
    """State machine for 3-leg arbitrage cycle."""

    SCANNING = "SCANNING"  # Initial: evaluating opportunities
    LEG1 = "LEG1"  # Executing buy order on source exchange
    LEG1_TIMEOUT = "LEG1_TIMEOUT"  # Buy order timed out (5s)
    LEG2_WAIT = "LEG2_WAIT"  # Waiting for withdrawal confirmation
    LEG2 = "LEG2"  # Executing withdrawal
    LEG3 = "LEG3"  # Executing sell order on destination exchange
    LEG3_MARKET = "LEG3_MARKET"  # Fell back to market order (timeout 10s)
    COMPLETE = "COMPLETE"  # Cycle completed successfully
    FAILED = "FAILED"  # Cycle failed at any stage
    CANCELLED = "CANCELLED"  # Manually cancelled


@dataclass
class OrderInfo:
    """Information about an order execution."""

    order_id: str
    order_type: str  # "limit" or "market"
    side: str  # "buy" or "sell"
    price: float
    amount: float
    filled: float = 0.0
    average_price: float = 0.0
    status: str = "open"  # "open", "closed", "cancelled", "timeout"
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "order_id": self.order_id,
            "order_type": self.order_type,
            "side": self.side,
            "price": self.price,
            "amount": self.amount,
            "filled": self.filled,
            "average_price": self.average_price,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class WithdrawalInfo:
    """Information about a withdrawal execution."""

    withdrawal_id: str
    currency: str
    network: str
    amount: float
    fee: float
    address: str = "dry_run_address"
    status: str = "pending"  # "pending", "confirmed", "completed", "failed"
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return {
            "withdrawal_id": self.withdrawal_id,
            "currency": self.currency,
            "network": self.network,
            "amount": self.amount,
            "fee": self.fee,
            "address": self.address,
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class CycleEvent:
    """Log event within a cycle."""

    timestamp: datetime
    state: CycleState
    message: str
    details: Optional[dict] = None

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        from typing import Any

        result: dict[str, Any] = {
            "timestamp": self.timestamp.isoformat(),
            "state": self.state.value,
            "message": self.message,
        }
        if self.details:
            result["details"] = self.details
        return result


@dataclass
class TradeCycle:
    """
    Complete 3-leg arbitrage trading cycle.

    Tracks the full lifecycle from opportunity detection through
    buy → withdrawal → sell execution.
    """

    # Unique identifier
    cycle_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    # State tracking
    state: CycleState = CycleState.SCANNING
    opportunity: Optional[Opportunity] = None

    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Execution details
    leg1_order: Optional[OrderInfo] = None
    leg2_withdrawal: Optional[WithdrawalInfo] = None
    leg3_order: Optional[OrderInfo] = None

    # Position tracking
    base_currency: Optional[str] = None  # e.g., "BTC" from "BTC/USDT"
    quote_currency: Optional[str] = None  # e.g., "USDT" from "BTC/USDT"
    position_amount: float = 0.0  # Amount of base currency held
    position_value_usd: float = 0.0  # USD value of position

    # Financial results
    total_fees_usd: float = 0.0
    realized_profit_usd: float = 0.0
    realized_profit_pct: float = 0.0

    # Event log
    events: list[CycleEvent] = field(default_factory=list)

    # Failure info
    failure_reason: Optional[str] = None

    def log_event(
        self, state: CycleState, message: str, details: Optional[dict] = None
    ) -> None:
        """Add an event to the cycle log."""
        event = CycleEvent(
            timestamp=datetime.utcnow(),
            state=state,
            message=message,
            details=details,
        )
        self.events.append(event)

    def duration_seconds(self) -> float:
        """Calculate cycle duration in seconds."""
        if not self.started_at:
            return 0.0
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        result = {
            "cycle_id": self.cycle_id,
            "state": self.state.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "duration_seconds": self.duration_seconds(),
        }

        # Add opportunity details
        if self.opportunity:
            result["opportunity"] = {
                "symbol": self.opportunity.symbol,
                "buy_exchange": self.opportunity.buy_exchange,
                "buy_price": self.opportunity.buy_price,
                "sell_exchange": self.opportunity.sell_exchange,
                "sell_price": self.opportunity.sell_price,
                "gross_spread_pct": self.opportunity.gross_spread_pct,
                "net_profit_pct": self.opportunity.net_profit_pct,
                "network": self.opportunity.network,
            }

        # Add execution details
        if self.leg1_order:
            result["leg1_order"] = self.leg1_order.to_dict()
        if self.leg2_withdrawal:
            result["leg2_withdrawal"] = self.leg2_withdrawal.to_dict()
        if self.leg3_order:
            result["leg3_order"] = self.leg3_order.to_dict()

        # Add position info
        result["position"] = {
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "amount": self.position_amount,
            "value_usd": self.position_value_usd,
        }

        # Add financial results
        result["results"] = {
            "total_fees_usd": self.total_fees_usd,
            "realized_profit_usd": self.realized_profit_usd,
            "realized_profit_pct": self.realized_profit_pct,
        }

        # Add events
        result["events"] = [event.to_dict() for event in self.events]

        # Add failure info if present
        if self.failure_reason:
            result["failure_reason"] = self.failure_reason

        return result

    def __repr__(self) -> str:
        """Human-readable representation."""
        parts = [f"TradeCycle({self.cycle_id})"]
        parts.append(f"state={self.state.value}")
        if self.opportunity:
            parts.append(f"symbol={self.opportunity.symbol}")
            parts.append(
                f"{self.opportunity.buy_exchange}→{self.opportunity.sell_exchange}"
            )
        if self.state in (CycleState.COMPLETE, CycleState.FAILED):
            parts.append(f"profit={self.realized_profit_pct:.2f}%")
        return " ".join(parts)
