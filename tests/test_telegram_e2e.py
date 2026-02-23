"""
E2E Tests for Telegram Bot Commands (Protocol 0002).

Tests all SimpleBot commands:
- /ping: Health check
- /status: Configuration display
- /cycles: List active cycles
- /confirm: Approve LEG2 withdrawal
- /cancel: Cancel active cycle

Plus happy path for full trading cycle.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def mock_settings():
    """Mock settings to provide required Telegram config."""
    with patch("parsertang.alerts.settings") as mock_s:
        mock_s.telegram_bot_token = "test_token_123"
        mock_s.telegram_chat_id = "12345"
        mock_s.get_access_control_ids = Mock(return_value={"12345"})
        mock_s.trading_enabled = False
        mock_s.dry_run_mode = True
        mock_s.current_phase = "R3"
        mock_s.max_concurrent_cycles = 1
        mock_s.ws_enabled = True
        mock_s.exchanges = ["binance", "bybit"]
        mock_s.min_net_profit = 0.3
        yield mock_s


@pytest.fixture
def mock_update():
    """Mock Telegram Update object."""
    update = AsyncMock()
    update.effective_chat = Mock()
    update.effective_chat.id = 12345  # Authorized chat ID
    update.message = AsyncMock()
    update.message.reply_text = AsyncMock()
    return update


@pytest.fixture
def unauthorized_update():
    """Mock Telegram Update from unauthorized user."""
    update = AsyncMock()
    update.effective_chat = Mock()
    update.effective_chat.id = 99999  # Wrong chat ID
    update.message = AsyncMock()
    update.message.reply_text = AsyncMock()
    return update


@pytest.fixture
def mock_context():
    """Mock Telegram context."""
    context = Mock()
    context.args = []
    return context


@pytest.fixture
def mock_app():
    """Mock Telegram Application."""
    with patch("parsertang.alerts.Application") as mock_app_class:
        mock_app = MagicMock()
        mock_app_class.builder.return_value.token.return_value.build.return_value = (
            mock_app
        )
        yield mock_app_class


@pytest.fixture
def mock_opportunity():
    """Create a mock arbitrage opportunity."""
    from parsertang.arbitrage import Opportunity

    return Opportunity(
        symbol="USDT/USD",
        buy_exchange="binance",
        buy_price=0.9995,
        sell_exchange="bybit",
        sell_price=1.0010,
        gross_spread_pct=0.15,
        trade_fees_pct=0.04,
        withdraw_fee_pct=0.01,
        net_profit_pct=0.10,
        bid_liq_usd=50000.0,
        ask_liq_usd=60000.0,
        network="TRC20",
    )


@pytest.fixture
def mock_trader(mock_opportunity):
    """Mock SimpleTrader with an active cycle."""

    trader = Mock()
    trader.active_cycles = {}
    trader.confirm_leg2_withdrawal = AsyncMock(return_value=True)
    trader.cancel_cycle = AsyncMock(return_value=True)

    return trader


@pytest.fixture
def mock_trader_with_cycle(mock_trader, mock_opportunity):
    """Mock trader with an active cycle in LEG2_WAIT state."""
    from parsertang.trade_models import TradeCycle, CycleState

    cycle = TradeCycle(opportunity=mock_opportunity)
    cycle.state = CycleState.LEG2_WAIT
    cycle.position_amount = 100.0

    mock_trader.active_cycles[cycle.cycle_id] = cycle
    return mock_trader, cycle


# =============================================================================
# /ping TESTS
# =============================================================================


class TestPingCommand:
    """Tests for /ping command."""

    @pytest.mark.asyncio
    async def test_ping_authorized(
        self, mock_settings, mock_update, mock_context, mock_app
    ):
        """Test /ping returns alive message for authorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()
        await bot.cmd_ping(mock_update, mock_context)

        mock_update.message.reply_text.assert_called_once_with("🟢 Bot is alive")

    @pytest.mark.asyncio
    async def test_ping_unauthorized(
        self, mock_settings, unauthorized_update, mock_context, mock_app
    ):
        """Test /ping rejects unauthorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()
        await bot.cmd_ping(unauthorized_update, mock_context)

        unauthorized_update.message.reply_text.assert_called_once_with("unauthorized")


# =============================================================================
# /status TESTS
# =============================================================================


class TestStatusCommand:
    """Tests for /status command."""

    @pytest.mark.asyncio
    async def test_status_authorized(
        self, mock_settings, mock_update, mock_context, mock_app
    ):
        """Test /status returns configuration for authorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()
        await bot.cmd_status(mock_update, mock_context)

        # Should be called with status JSON
        call_args = mock_update.message.reply_text.call_args
        message = call_args[0][0]

        assert "Status" in message
        assert "ws_enabled" in message
        assert "exchanges" in message

    @pytest.mark.asyncio
    async def test_status_unauthorized(
        self, mock_settings, unauthorized_update, mock_context, mock_app
    ):
        """Test /status rejects unauthorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()
        await bot.cmd_status(unauthorized_update, mock_context)

        unauthorized_update.message.reply_text.assert_called_once_with("unauthorized")


# =============================================================================
# /cycles TESTS
# =============================================================================


class TestCyclesCommand:
    """Tests for /cycles command."""

    @pytest.mark.asyncio
    async def test_cycles_no_trader(
        self, mock_settings, mock_update, mock_context, mock_app
    ):
        """Test /cycles without trader instance."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()  # No trader
        await bot.cmd_cycles(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "No trader" in call_args

    @pytest.mark.asyncio
    async def test_cycles_empty(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader
    ):
        """Test /cycles with no active cycles."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        await bot.cmd_cycles(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "No active cycles" in call_args

    @pytest.mark.asyncio
    async def test_cycles_with_active(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /cycles with active cycle."""
        from parsertang.alerts import SimpleBot

        trader, cycle = mock_trader_with_cycle
        bot = SimpleBot(trader=trader)
        await bot.cmd_cycles(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Active Cycles" in call_args
        assert cycle.cycle_id in call_args


# =============================================================================
# /confirm TESTS
# =============================================================================


class TestConfirmCommand:
    """Tests for /confirm command."""

    @pytest.mark.asyncio
    async def test_confirm_no_args(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader
    ):
        """Test /confirm without cycle_id shows usage."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = []

        await bot.cmd_confirm(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Usage: /confirm" in call_args

    @pytest.mark.asyncio
    async def test_confirm_cycle_not_found(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader
    ):
        """Test /confirm with non-existent cycle_id."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = ["nonexistent_id"]

        await bot.cmd_confirm(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "not found" in call_args

    @pytest.mark.asyncio
    async def test_confirm_success(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /confirm with valid cycle_id in LEG2_WAIT state."""
        from parsertang.alerts import SimpleBot

        trader, cycle = mock_trader_with_cycle
        bot = SimpleBot(trader=trader)
        mock_context.args = [cycle.cycle_id]

        await bot.cmd_confirm(mock_update, mock_context)

        # Should call confirm and return success message
        trader.confirm_leg2_withdrawal.assert_called_once_with(cycle.cycle_id)
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "confirmed" in call_args.lower()

    @pytest.mark.asyncio
    async def test_confirm_wrong_state(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /confirm when cycle is not in LEG2_WAIT state."""
        from parsertang.alerts import SimpleBot
        from parsertang.trade_models import CycleState

        trader, cycle = mock_trader_with_cycle
        cycle.state = CycleState.LEG1  # Wrong state

        bot = SimpleBot(trader=trader)
        mock_context.args = [cycle.cycle_id]

        await bot.cmd_confirm(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "not LEG2_WAIT" in call_args


# =============================================================================
# /cancel TESTS
# =============================================================================


class TestCancelCommand:
    """Tests for /cancel command."""

    @pytest.mark.asyncio
    async def test_cancel_no_args(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader
    ):
        """Test /cancel without cycle_id shows usage."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = []

        await bot.cmd_cancel(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Usage: /cancel" in call_args

    @pytest.mark.asyncio
    async def test_cancel_cycle_not_found(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader
    ):
        """Test /cancel with non-existent cycle_id."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = ["nonexistent_id"]

        await bot.cmd_cancel(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "not found" in call_args

    @pytest.mark.asyncio
    async def test_cancel_success(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /cancel with valid cycle_id."""
        from parsertang.alerts import SimpleBot

        trader, cycle = mock_trader_with_cycle
        bot = SimpleBot(trader=trader)
        mock_context.args = [cycle.cycle_id]

        await bot.cmd_cancel(mock_update, mock_context)

        # Should call cancel
        trader.cancel_cycle.assert_called_once_with(cycle.cycle_id)
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "cancelled" in call_args.lower()


# =============================================================================
# EDGE CASES & FAILURE MODES
# =============================================================================


class TestEdgeCases:
    """Edge cases and failure mode tests."""

    @pytest.mark.asyncio
    async def test_confirm_trader_returns_failure(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /confirm when trader.confirm_leg2_withdrawal returns False."""
        from parsertang.alerts import SimpleBot

        trader, cycle = mock_trader_with_cycle
        trader.confirm_leg2_withdrawal = AsyncMock(return_value=False)

        bot = SimpleBot(trader=trader)
        mock_context.args = [cycle.cycle_id]

        await bot.cmd_confirm(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Failed" in call_args or "failed" in call_args.lower()

    @pytest.mark.asyncio
    async def test_cancel_trader_returns_failure(
        self, mock_settings, mock_update, mock_context, mock_app, mock_trader_with_cycle
    ):
        """Test /cancel when trader.cancel_cycle returns False."""
        from parsertang.alerts import SimpleBot

        trader, cycle = mock_trader_with_cycle
        trader.cancel_cycle = AsyncMock(return_value=False)

        bot = SimpleBot(trader=trader)
        mock_context.args = [cycle.cycle_id]

        await bot.cmd_cancel(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Failed" in call_args or "failed" in call_args.lower()

    @pytest.mark.asyncio
    async def test_confirm_no_trader(
        self, mock_settings, mock_update, mock_context, mock_app
    ):
        """Test /confirm without trader instance."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()  # No trader
        mock_context.args = ["some_cycle_id"]

        await bot.cmd_confirm(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "No trader" in call_args

    @pytest.mark.asyncio
    async def test_cancel_no_trader(
        self, mock_settings, mock_update, mock_context, mock_app
    ):
        """Test /cancel without trader instance."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot()  # No trader
        mock_context.args = ["some_cycle_id"]

        await bot.cmd_cancel(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "No trader" in call_args

    @pytest.mark.asyncio
    async def test_cycles_unauthorized(
        self, mock_settings, unauthorized_update, mock_context, mock_app, mock_trader
    ):
        """Test /cycles rejects unauthorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        await bot.cmd_cycles(unauthorized_update, mock_context)

        unauthorized_update.message.reply_text.assert_called_once_with("unauthorized")

    @pytest.mark.asyncio
    async def test_confirm_unauthorized(
        self, mock_settings, unauthorized_update, mock_context, mock_app, mock_trader
    ):
        """Test /confirm rejects unauthorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = ["some_cycle_id"]

        await bot.cmd_confirm(unauthorized_update, mock_context)

        unauthorized_update.message.reply_text.assert_called_once_with("unauthorized")

    @pytest.mark.asyncio
    async def test_cancel_unauthorized(
        self, mock_settings, unauthorized_update, mock_context, mock_app, mock_trader
    ):
        """Test /cancel rejects unauthorized user."""
        from parsertang.alerts import SimpleBot

        bot = SimpleBot(trader=mock_trader)
        mock_context.args = ["some_cycle_id"]

        await bot.cmd_cancel(unauthorized_update, mock_context)

        unauthorized_update.message.reply_text.assert_called_once_with("unauthorized")

    @pytest.mark.asyncio
    async def test_cycles_multiple_active(
        self, mock_settings, mock_update, mock_context, mock_app, mock_opportunity
    ):
        """Test /cycles with multiple active cycles."""
        from parsertang.alerts import SimpleBot
        from parsertang.trade_models import TradeCycle, CycleState

        trader = Mock()

        # Create multiple cycles
        cycle1 = TradeCycle(opportunity=mock_opportunity)
        cycle1.state = CycleState.LEG2_WAIT

        cycle2 = TradeCycle(opportunity=mock_opportunity)
        cycle2.state = CycleState.LEG1

        trader.active_cycles = {
            cycle1.cycle_id: cycle1,
            cycle2.cycle_id: cycle2,
        }

        bot = SimpleBot(trader=trader)
        await bot.cmd_cycles(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Active Cycles" in call_args
        assert cycle1.cycle_id in call_args
        assert cycle2.cycle_id in call_args
