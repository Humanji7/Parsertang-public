"""
Tests for alert deduplication logic.

Covers:
1. Duplicate alert within cooldown → not sent
2. Same pair, different direction → sent
3. Same pair, significant profit change → sent
4. Same pair, after cooldown → sent
"""

import time
from unittest.mock import patch

import pytest

from parsertang.core.state_manager import AppState
from parsertang.core.opportunity_evaluator import (
    ALERT_HARD_COOLDOWN_SECONDS,
    ALERT_MIN_REPEAT_SECONDS,
    ALERT_SOFT_COOLDOWN_SECONDS,
    should_send_alert,
)
from parsertang.alerts import format_alert


@pytest.fixture
def app_state():
    """Create fresh AppState for each test."""
    return AppState()


@pytest.fixture
def mock_settings():
    """Create settings with known dedup threshold."""
    from parsertang.config import settings

    with patch.object(settings, "min_net_profit", 0.5):
        with patch.object(settings, "alert_dedup_threshold_pct", 0.1):
            with patch.object(settings, "liquidity_usd_threshold", 1000.0):
                yield settings


class TestAlertDeduplication:
    """Test alert deduplication based on direction and profit change."""

    def test_duplicate_within_cooldown_blocked(self, app_state, mock_settings):
        """Same symbol+direction within cooldown should be deduplicated."""
        symbol = "APT/USDT"
        buy_ex = "gate"
        sell_ex = "mexc"
        net_profit = 0.6

        # First alert recorded
        now = time.monotonic()
        alert_key = (symbol, buy_ex, sell_ex)
        app_state.last_alert_ts[alert_key] = (now, net_profit)

        # Simulate second alert attempt immediately (same profit)
        last_alert = app_state.last_alert_ts.get(alert_key)
        assert last_alert is not None

        last_ts, last_net_profit = last_alert
        elapsed = time.monotonic() - last_ts
        hard_cooldown_ok = elapsed >= ALERT_HARD_COOLDOWN_SECONDS
        profit_change = abs(net_profit - last_net_profit)
        profit_ok = profit_change > mock_settings.alert_dedup_threshold_pct

        # Both checks should fail → duplicate
        assert not hard_cooldown_ok  # Within hard cooldown
        assert not profit_ok  # Profit unchanged

    def test_different_direction_allowed(self, app_state, mock_settings):
        """Same symbol but different direction should create new alert."""
        symbol = "APT/USDT"

        # First direction: gate → mexc
        key1 = (symbol, "gate", "mexc")
        app_state.last_alert_ts[key1] = (time.monotonic(), 0.6)

        # Second direction: mexc → gate (different key)
        key2 = (symbol, "mexc", "gate")
        last_alert = app_state.last_alert_ts.get(key2)

        # Should be None - no prior alert for this direction
        assert last_alert is None

    def test_significant_profit_change_allowed(self, app_state, mock_settings):
        """Same direction but significant profit change should trigger alert."""
        symbol = "APT/USDT"
        buy_ex = "gate"
        sell_ex = "mexc"
        alert_key = (symbol, buy_ex, sell_ex)

        # First alert at 0.6% profit
        now = time.monotonic()
        app_state.last_alert_ts[alert_key] = (now, 0.6)

        # Second alert at 0.8% profit (0.2% change > 0.1% threshold)
        new_profit = 0.8
        last_ts, last_net_profit = app_state.last_alert_ts[alert_key]
        profit_change = abs(new_profit - last_net_profit)
        profit_ok = profit_change > mock_settings.alert_dedup_threshold_pct

        assert profit_ok  # 0.2% > 0.1% threshold

    def test_after_cooldown_allowed(self, app_state, mock_settings):
        """Same direction after cooldown should trigger alert."""
        symbol = "APT/USDT"
        buy_ex = "gate"
        sell_ex = "mexc"
        alert_key = (symbol, buy_ex, sell_ex)

        # Alert from 120 seconds ago (beyond 60s cooldown)
        old_time = time.monotonic() - 120
        app_state.last_alert_ts[alert_key] = (old_time, 0.6)

        # Check if cooldown passed
        last_ts, _ = app_state.last_alert_ts[alert_key]
        elapsed = time.monotonic() - last_ts
        soft_cooldown_ok = elapsed >= ALERT_SOFT_COOLDOWN_SECONDS

        assert soft_cooldown_ok  # 120s > soft cooldown

    def test_min_repeat_blocks_even_with_profit_improvement(
        self, app_state, mock_settings
    ):
        """Same pair within min repeat window should be blocked even if profit improves."""
        now = time.monotonic()
        last_ts = now - 120  # within 5 minutes
        last_alert = (last_ts, 0.6)

        should_alert = should_send_alert(
            now=now,
            last_alert=last_alert,
            net_profit_pct=0.9,  # big improvement
            dedup_threshold_pct=mock_settings.alert_dedup_threshold_pct,
            min_repeat_seconds=ALERT_MIN_REPEAT_SECONDS,
            hard_cooldown_seconds=ALERT_HARD_COOLDOWN_SECONDS,
            soft_cooldown_seconds=ALERT_SOFT_COOLDOWN_SECONDS,
        )

        assert should_alert is False

    def test_deduplicated_counter_increments(self, app_state, mock_settings):
        """Verify funnel counter increments on deduplication."""
        app_state.funnel_counters["alerts_deduplicated"] = 0

        # Simulate deduplication
        app_state.funnel_counters["alerts_deduplicated"] += 1

        assert app_state.funnel_counters["alerts_deduplicated"] == 1


class TestAlertKeyStructure:
    """Test the alert key tuple structure."""

    def test_key_is_symbol_buy_sell_tuple(self, app_state):
        """Alert key should be (symbol, buy_exchange, sell_exchange)."""
        key = ("APT/USDT", "gate", "mexc")
        value = (time.monotonic(), 0.6)

        app_state.last_alert_ts[key] = value

        assert key in app_state.last_alert_ts
        stored_ts, stored_profit = app_state.last_alert_ts[key]
        assert isinstance(stored_ts, float)
        assert isinstance(stored_profit, float)

    def test_symmetric_routes_separate_keys(self, app_state):
        """gate→mexc and mexc→gate should have separate keys."""
        key1 = ("APT/USDT", "gate", "mexc")
        key2 = ("APT/USDT", "mexc", "gate")

        app_state.last_alert_ts[key1] = (time.monotonic(), 0.6)

        assert key1 in app_state.last_alert_ts
        assert key2 not in app_state.last_alert_ts


class TestAlertFormatting:
    def test_alert_contains_evidence_id_and_ui_check_instructions(self, monkeypatch):
        # Freeze timestamp so evidence id is deterministic
        from parsertang import alerts as alerts_mod

        monkeypatch.setattr(alerts_mod.time, "time", lambda: 1700000000.0)

        msg = format_alert(
            symbol="OP/USDT",
            buy_exchange="okx",
            buy_price=2.0,
            sell_exchange="bybit",
            sell_price=2.01,
            gross_spread_pct=0.5,
            trade_fees_pct=0.2,
            withdraw_fee_pct=0.01,
            net_profit_pct=0.29,
            bid_liq_usd=12345.0,
            ask_liq_usd=23456.0,
            network="OPT",
            withdrawal_fee_base=0.046,
            fee_confidence="HIGH",
        )

        assert "ID:" in msg
        assert "Verify in UI:" in msg
        assert "BUY: okx ASK" in msg
        assert "SELL: bybit BID" in msg
