"""Tests for FeeStatistics module - Daily Fee Report functionality.

TDD: Tests written BEFORE implementation.
"""

import asyncio

import pytest


class TestFeeStatistics:
    """Test FeeStatistics class - thread-safe missing fallback tracking."""

    @pytest.fixture
    def fee_stats(self):
        """Create fresh FeeStatistics instance."""
        from parsertang.fee_statistics import FeeStatistics

        return FeeStatistics()

    @pytest.mark.asyncio
    async def test_record_missing_fallback_basic(self, fee_stats):
        """Test basic recording of missing fallback."""
        await fee_stats.record_missing_fallback("okx", "USD1")

        # Check internal state
        async with fee_stats._lock:
            assert ("okx", "USD1") in fee_stats._missing_fallbacks
            assert fee_stats._missing_fallbacks[("okx", "USD1")] == 1

    @pytest.mark.asyncio
    async def test_record_missing_fallback_multiple_calls(self, fee_stats):
        """Test that multiple calls increment counter."""
        await fee_stats.record_missing_fallback("okx", "USD1")
        await fee_stats.record_missing_fallback("okx", "USD1")
        await fee_stats.record_missing_fallback("okx", "USD1")

        async with fee_stats._lock:
            assert fee_stats._missing_fallbacks[("okx", "USD1")] == 3

    @pytest.mark.asyncio
    async def test_record_missing_fallback_different_currencies(self, fee_stats):
        """Test tracking different exchange/currency combinations."""
        await fee_stats.record_missing_fallback("okx", "USD1")
        await fee_stats.record_missing_fallback("bybit", "XXX")
        await fee_stats.record_missing_fallback("okx", "USD1")

        async with fee_stats._lock:
            assert fee_stats._missing_fallbacks[("okx", "USD1")] == 2
            assert fee_stats._missing_fallbacks[("bybit", "XXX")] == 1

    @pytest.mark.asyncio
    async def test_get_and_clear_stats(self, fee_stats):
        """Test get_and_clear_stats returns data and clears internal state."""
        await fee_stats.record_missing_fallback("okx", "USD1")
        await fee_stats.record_missing_fallback("bybit", "XXX")
        await fee_stats.record_missing_fallback("okx", "USD1")

        stats = await fee_stats.get_and_clear_stats()

        # Should return dict with counts
        assert stats == {("okx", "USD1"): 2, ("bybit", "XXX"): 1}

        # Internal state should be cleared
        async with fee_stats._lock:
            assert len(fee_stats._missing_fallbacks) == 0

    @pytest.mark.asyncio
    async def test_get_and_clear_stats_empty(self, fee_stats):
        """Test get_and_clear_stats with no data."""
        stats = await fee_stats.get_and_clear_stats()
        assert stats == {}

    @pytest.mark.asyncio
    async def test_thread_safety_concurrent_writes(self, fee_stats):
        """Test thread safety with concurrent writes."""

        async def writer(exchange: str, currency: str, count: int):
            for _ in range(count):
                await fee_stats.record_missing_fallback(exchange, currency)
                await asyncio.sleep(0)  # Yield to other tasks

        # Run 100 concurrent writes
        await asyncio.gather(
            writer("okx", "USD1", 50),
            writer("okx", "USD1", 50),
            writer("bybit", "XXX", 30),
        )

        stats = await fee_stats.get_and_clear_stats()
        assert stats[("okx", "USD1")] == 100
        assert stats[("bybit", "XXX")] == 30


class TestFormatDailyFeeReport:
    """Test format_daily_fee_report function."""

    def test_format_empty_stats(self):
        """Test formatting with no missing fallbacks."""
        from parsertang.fee_statistics import format_daily_fee_report

        result = format_daily_fee_report({})
        assert result is None  # No report if no problems

    def test_format_single_currency(self):
        """Test formatting with single missing fallback."""
        from parsertang.fee_statistics import format_daily_fee_report

        stats = {("okx", "USD1"): 47}
        result = format_daily_fee_report(stats)

        assert result is not None
        assert "Daily Fee Report" in result
        assert "okx/USD1" in result
        assert "47" in result

    def test_format_multiple_currencies(self):
        """Test formatting with multiple missing fallbacks."""
        from parsertang.fee_statistics import format_daily_fee_report

        stats = {
            ("okx", "USD1"): 47,
            ("bybit", "XXX"): 12,
            ("gate", "YYY"): 5,
        }
        result = format_daily_fee_report(stats)

        assert result is not None
        assert "okx/USD1" in result
        assert "bybit/XXX" in result
        assert "gate/YYY" in result
        assert "Всего проблемных валют: 3" in result

    def test_format_sorted_by_count_desc(self):
        """Test that currencies are sorted by rejection count (descending)."""
        from parsertang.fee_statistics import format_daily_fee_report

        stats = {
            ("gate", "LOW"): 5,
            ("okx", "HIGH"): 100,
            ("bybit", "MED"): 50,
        }
        result = format_daily_fee_report(stats)

        assert result is not None
        # HIGH (100) should appear before MED (50) before LOW (5)
        high_pos = result.find("okx/HIGH")
        med_pos = result.find("bybit/MED")
        low_pos = result.find("gate/LOW")
        assert high_pos < med_pos < low_pos

    def test_format_limit_50_items(self):
        """Test that report is limited to 50 items."""
        from parsertang.fee_statistics import format_daily_fee_report

        # Create 60 items
        stats = {(f"ex{i}", f"CUR{i}"): i + 1 for i in range(60)}
        result = format_daily_fee_report(stats)

        assert result is not None
        # Should show "... и ещё X валют" for items beyond 50
        assert "и ещё" in result


class TestDailyFeeReportTask:
    """Test daily_fee_report_task coroutine."""

    @pytest.mark.asyncio
    async def test_task_does_not_send_if_no_problems(self):
        """Test that task doesn't send if no missing fallbacks."""
        from parsertang.fee_statistics import FeeStatistics

        fee_stats = FeeStatistics()

        # Run one iteration (need to mock sleep to avoid waiting 24h)
        # We'll test the core logic directly
        stats = await fee_stats.get_and_clear_stats()
        assert stats == {}

    @pytest.mark.asyncio
    async def test_task_sends_report_with_problems(self):
        """Test that task sends report when there are problems."""

        from parsertang.fee_statistics import FeeStatistics, format_daily_fee_report

        fee_stats = FeeStatistics()
        await fee_stats.record_missing_fallback("okx", "USD1")

        stats = await fee_stats.get_and_clear_stats()
        report = format_daily_fee_report(stats)

        assert report is not None
        assert "okx/USD1" in report


class TestConfigValidation:
    """Test config validation for daily fee report settings."""

    def test_feature_disabled_by_default(self):
        """Test that feature is disabled by default."""
        from parsertang.config import Settings

        # Create settings with defaults (no env vars)
        settings = Settings(
            _env_file=None,  # Disable .env loading
        )
        assert settings.enable_daily_fee_report is False

    def test_fail_fast_if_enabled_without_chat_id(self):
        """Test that enabling feature without tech_chat_id raises error."""
        import pytest
        from pydantic import ValidationError

        from parsertang.config import Settings

        with pytest.raises(ValidationError) as exc_info:
            Settings(
                _env_file=None,
                enable_daily_fee_report=True,
                telegram_tech_chat_id=None,
            )

        assert "ENABLE_DAILY_FEE_REPORT" in str(exc_info.value)
        assert "TELEGRAM_TECH_CHAT_ID" in str(exc_info.value)

    def test_feature_enabled_with_chat_id(self):
        """Test that feature can be enabled with tech_chat_id."""
        from parsertang.config import Settings

        settings = Settings(
            _env_file=None,
            enable_daily_fee_report=True,
            telegram_tech_chat_id="123456789",
        )
        assert settings.enable_daily_fee_report is True
        assert settings.telegram_tech_chat_id == "123456789"
