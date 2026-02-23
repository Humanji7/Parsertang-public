"""Tests for SPEC-FALLBACK-001: Static withdrawal fees fallback system."""

import unittest.mock


class TestStaticFeeDataclass:
    """Phase 1: Static fees data structure tests."""

    def test_static_fee_import_and_create(self) -> None:
        """Step 1.1: StaticFee dataclass can be imported and instantiated."""
        from parsertang.static_withdrawal_fees import StaticFee

        fee = StaticFee(fee=0.004, source="okx_website", verified_at="2025-01-03")

        assert fee.fee == 0.004
        assert fee.source == "okx_website"
        assert fee.verified_at == "2025-01-03"

    def test_fallback_fees_dict_exists(self) -> None:
        """Step 1.3: FALLBACK_FEES dict exists with correct type signature."""
        from parsertang.static_withdrawal_fees import FALLBACK_FEES, StaticFee

        # Check it's a dict
        assert isinstance(FALLBACK_FEES, dict)

        # Check type signature: keys are (exchange, currency, network) tuples
        for key, value in FALLBACK_FEES.items():
            assert isinstance(key, tuple)
            assert len(key) == 3
            assert all(isinstance(k, str) for k in key)
            assert isinstance(value, StaticFee)

    def test_fallback_fees_contains_okx_usdc_arb(self) -> None:
        """Step 1.5: FALLBACK_FEES contains OKX USDC/ARB with correct fee."""
        from parsertang.static_withdrawal_fees import FALLBACK_FEES

        key = ("okx", "USDC", "ARB")
        assert key in FALLBACK_FEES, f"Missing key {key}"

        fee = FALLBACK_FEES[key]
        assert fee.fee == 0.004
        assert fee.source == "okx_website"
        assert fee.verified_at == "2025-01-03"


class TestGetFallbackFee:
    """Phase 2: get_fallback_fee() function tests."""

    def test_get_fallback_fee_signature(self) -> None:
        """Step 2.1: get_fallback_fee() can be imported and has correct signature."""
        from parsertang.static_withdrawal_fees import StaticFee, get_fallback_fee

        # Should accept (exchange, currency, network) and return StaticFee | None
        result = get_fallback_fee("okx", "USDC", "ARB")
        assert result is None or isinstance(result, StaticFee)

    def test_get_fallback_fee_returns_data(self) -> None:
        """Step 2.3: get_fallback_fee() returns StaticFee for known entry."""
        from parsertang.static_withdrawal_fees import get_fallback_fee

        result = get_fallback_fee("okx", "USDC", "ARB")

        assert result is not None
        assert result.fee == 0.004
        assert result.source == "okx_website"
        assert result.verified_at == "2025-01-03"

    def test_get_fallback_fee_returns_none_for_missing(self) -> None:
        """Step 2.5: get_fallback_fee() returns None for unknown exchange."""
        from parsertang.static_withdrawal_fees import get_fallback_fee

        result = get_fallback_fee("binance", "USDC", "ARB")

        assert result is None

    def test_get_fallback_fee_case_insensitive(self) -> None:
        """Step 2.6: get_fallback_fee() handles mixed case input."""
        from parsertang.static_withdrawal_fees import get_fallback_fee

        result = get_fallback_fee("OKX", "usdc", "arb")

        assert result is not None
        assert result.fee == 0.004


class TestWithdrawalFeeSignature:
    """Phase 3: get_withdrawal_fee() signature change tests."""

    def test_get_withdrawal_fee_returns_tuple(self) -> None:
        """Step 3.1: get_withdrawal_fee() returns tuple[float|None, str]."""
        from unittest.mock import MagicMock

        from parsertang.withdrawal_fees import WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Mock cache with data
        mock_cache = MagicMock()
        mock_cache.get_fee.return_value = 0.5
        mock_cache.fees = {"okx": {"USDT": {"TRC20": 0.5}}}
        manager.cache = mock_cache

        result = manager.get_withdrawal_fee("okx", "USDT", "TRC20")

        assert isinstance(result, tuple)
        assert len(result) == 2
        fee, confidence = result
        assert fee == 0.5
        assert confidence == "HIGH"


class TestRaceConditionProtection:
    """Phase 4: Race condition protection tests."""

    def test_get_withdrawal_fee_with_none_cache(self) -> None:
        """Step 4.1: get_withdrawal_fee() returns (None, 'HIGH') when cache is None."""
        from parsertang.withdrawal_fees import WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        manager.cache = None  # Explicitly set to None

        fee, confidence = manager.get_withdrawal_fee("okx", "USDT", "TRC20")

        assert fee is None
        assert confidence == "HIGH"


class TestFallbackIntegration:
    """Phase 5: Fallback logic integration tests."""

    def test_fallback_when_currency_missing_from_cache(self) -> None:
        """Step 5.1: Fallback is used when currency is missing from populated cache."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Cache has OKX data but NOT for USDC
        manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {
                    "USDT": {"TRC20": 1.0},  # Has USDT
                    # No USDC
                },
            },
            last_updated=time.time(),
        )

        # USDC/ARB is in FALLBACK_FEES for OKX
        fee, confidence = manager.get_withdrawal_fee("okx", "USDC", "ARB")

        assert fee == 0.004  # From FALLBACK_FEES
        assert confidence == "MEDIUM"

    def test_no_fallback_when_exchange_cache_empty(self) -> None:
        """Step 5.3: No fallback when exchange cache is empty (API down)."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Cache exists but OKX has no data (API down scenario)
        manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {},  # Empty - API likely down
                "bybit": {"USDT": {"TRC20": 1.0}},  # Other exchange has data
            },
            last_updated=time.time(),
        )

        # Even though USDC/ARB is in FALLBACK_FEES, don't use it when API is down
        fee, confidence = manager.get_withdrawal_fee("okx", "USDC", "ARB")

        assert fee is None
        assert confidence == "HIGH"

    def test_fallback_logs_info_message(self) -> None:
        """Step 5.5: Fallback usage is logged at INFO level."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        manager.cache = WithdrawalFeeCache(
            fees={"okx": {"USDT": {"TRC20": 1.0}}},
            last_updated=time.time(),
        )

        with unittest.mock.patch("parsertang.withdrawal_fees.logger") as mock_logger:
            manager.get_withdrawal_fee("okx", "USDC", "ARB")

            # Check that info was called with fallback message
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "FEE FALLBACK" in call_args
            assert "okx" in call_args
            assert "USDC" in call_args


class TestFeeCalculatorIntegration:
    """Phase 6: fee_calculator.py integration tests."""

    def test_fee_calculation_result_receives_confidence(self) -> None:
        """Step 6.1: FeeCalculationResult.fee_confidence is set from get_withdrawal_fee()."""
        import time

        from parsertang.core.fee_calculator import (
            calculate_opportunity_fees_and_network,
        )
        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        # Create fee manager with fallback scenario
        fee_manager = WithdrawalFeeManager(exchanges={})
        fee_manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {"USDT": {"TRC20": 1.0}},  # Has USDT, not USDC
                "bybit": {"USDT": {"TRC20": 0.8}},
            },
            last_updated=time.time(),
        )

        # Mock state with necessary attributes
        state = unittest.mock.MagicMock()
        state.fee_manager = fee_manager
        state.currency_cache = {
            "okx": {"USDC": {"networks": {"ARB": {"withdraw": {"active": True}}}}},
            "bybit": {"USDC": {"networks": {"ARB": {"withdraw": {"active": True}}}}},
        }

        with unittest.mock.patch(
            "parsertang.core.fee_calculator.get_taker_fee", return_value=0.1
        ):
            result = calculate_opportunity_fees_and_network(
                symbol="USDC/USDT",
                buy_exchange="okx",  # Will use fallback for USDC
                sell_exchange="bybit",
                best_ask=1.0,
                best_bid=1.005,
                state=state,
            )

        # Fallback was used for OKX USDC/ARB
        assert result.fee_confidence == "MEDIUM"


class TestFeeAgeDisplay:
    """Phase 7: Telegram alert age display tests."""

    def test_calculate_fee_age_days(self) -> None:
        """Step 7.1: calculate_fee_age_days() computes days since verification."""
        from datetime import date
        from unittest.mock import patch

        from parsertang.static_withdrawal_fees import calculate_fee_age_days

        # Verified on 2025-01-01, current date is 2025-01-03 -> 2 days
        with patch("parsertang.static_withdrawal_fees.date") as mock_date:
            mock_date.today.return_value = date(2025, 1, 3)
            mock_date.fromisoformat = date.fromisoformat

            age = calculate_fee_age_days("2025-01-01")
            assert age == 2

    def test_get_fee_age_warning_no_warning_for_fresh_data(self) -> None:
        """Step 7.3: No warning for data <= 3 days old."""
        from parsertang.static_withdrawal_fees import get_fee_age_warning

        # 0-3 days: no warning (empty string)
        assert get_fee_age_warning(0) == ""
        assert get_fee_age_warning(1) == ""
        assert get_fee_age_warning(2) == ""
        assert get_fee_age_warning(3) == ""

    def test_get_fee_age_warning_yellow_for_medium_age(self) -> None:
        """Step 7.3: Yellow warning for data 4-7 days old."""
        from parsertang.static_withdrawal_fees import get_fee_age_warning

        # 4-7 days: yellow warning
        assert "⚠️" in get_fee_age_warning(4)
        assert "⚠️" in get_fee_age_warning(5)
        assert "⚠️" in get_fee_age_warning(7)

    def test_get_fee_age_warning_red_for_stale_data(self) -> None:
        """Step 7.3: Red warning for data > 7 days old."""
        from parsertang.static_withdrawal_fees import get_fee_age_warning

        # >7 days: red warning
        assert "🔴" in get_fee_age_warning(8)
        assert "🔴" in get_fee_age_warning(14)
        assert "🔴" in get_fee_age_warning(30)

    def test_format_alert_shows_age_for_medium_confidence(self) -> None:
        """Step 7.5: format_alert() shows fee age warning for MEDIUM confidence."""
        from datetime import date
        from unittest.mock import patch

        from parsertang.alerts import format_alert

        # Patch date.today() to return fixed date
        with patch("parsertang.static_withdrawal_fees.date") as mock_date:
            mock_date.today.return_value = date(
                2025, 1, 10
            )  # 7 days after verification
            mock_date.fromisoformat = date.fromisoformat

            message = format_alert(
                symbol="USDC/USDT",
                buy_exchange="okx",
                buy_price=1.0,
                sell_exchange="bybit",
                sell_price=1.005,
                gross_spread_pct=0.5,
                trade_fees_pct=0.2,
                withdraw_fee_pct=0.1,
                net_profit_pct=0.2,
                bid_liq_usd=10000.0,
                ask_liq_usd=15000.0,
                network="ARB",
                withdrawal_fee_base=0.004,
                fee_confidence="MEDIUM",
                fee_verified_at="2025-01-03",  # New parameter
            )

        # Should contain age warning for 7-day-old data
        assert "⚠️" in message
        assert "7 days old" in message

    def test_format_alert_no_age_for_high_confidence(self) -> None:
        """Step 7.5: format_alert() does not show age for HIGH confidence."""
        from parsertang.alerts import format_alert

        message = format_alert(
            symbol="USDC/USDT",
            buy_exchange="okx",
            buy_price=1.0,
            sell_exchange="bybit",
            sell_price=1.005,
            gross_spread_pct=0.5,
            trade_fees_pct=0.2,
            withdraw_fee_pct=0.1,
            net_profit_pct=0.2,
            bid_liq_usd=10000.0,
            ask_liq_usd=15000.0,
            network="ARB",
            withdrawal_fee_base=0.004,
            fee_confidence="HIGH",
            fee_verified_at="2025-01-03",  # Should be ignored for HIGH
        )

        # Should NOT contain age warning
        assert "days old" not in message


class TestDriftDetection:
    """Phase 8: Drift detection tests."""

    def test_check_fee_drift_detects_difference_over_10_percent(self) -> None:
        """Step 8.1: check_fee_drift() detects drift > 10%."""
        from parsertang.static_withdrawal_fees import check_fee_drift

        # Fallback: 0.004, Dynamic: 0.005 -> 25% drift
        cache_fees = {
            "okx": {
                "USDC": {"ARB": 0.005},  # 25% higher than fallback 0.004
            }
        }

        drifts = check_fee_drift(cache_fees)

        assert len(drifts) == 1
        drift = drifts[0]
        assert drift["exchange"] == "okx"
        assert drift["currency"] == "USDC"
        assert drift["network"] == "ARB"
        assert drift["fallback_fee"] == 0.004
        assert drift["dynamic_fee"] == 0.005
        assert drift["drift_pct"] == 25.0

    def test_check_fee_drift_ignores_small_differences(self) -> None:
        """Step 8.1: check_fee_drift() ignores drift <= 10%."""
        from parsertang.static_withdrawal_fees import check_fee_drift

        # Fallback: 0.004, Dynamic: 0.0043 -> 7.5% drift (ignored)
        cache_fees = {
            "okx": {
                "USDC": {"ARB": 0.0043},  # 7.5% higher - should be ignored
            }
        }

        drifts = check_fee_drift(cache_fees)

        assert len(drifts) == 0

    def test_check_fee_drift_returns_empty_for_no_overlaps(self) -> None:
        """Step 8.1: check_fee_drift() returns empty list when no overlaps."""
        from parsertang.static_withdrawal_fees import check_fee_drift

        # Cache has different currencies than fallback
        cache_fees = {
            "binance": {
                "USDT": {"TRC20": 1.0},
            }
        }

        drifts = check_fee_drift(cache_fees)

        assert len(drifts) == 0

    def test_refresh_cache_calls_drift_check_and_logs_errors(self) -> None:
        """Step 8.3: refresh_cache() calls check_fee_drift() and logs drift errors."""
        import asyncio

        from parsertang.withdrawal_fees import WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})

        # Mock fetch_all_fees to return drift data
        async def mock_fetch():
            return {
                "okx": {
                    "USDC": {"ARB": 0.005},  # 25% higher than fallback 0.004
                }
            }

        with (
            unittest.mock.patch.object(
                manager, "fetch_all_fees", side_effect=mock_fetch
            ),
            unittest.mock.patch("parsertang.withdrawal_fees.logger") as mock_logger,
        ):
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(manager.refresh_cache())
            finally:
                loop.close()

            # Verify drift was detected and logged as error
            mock_logger.error.assert_called()
            call_args = str(mock_logger.error.call_args)
            assert "FEE DRIFT" in call_args
            assert "okx" in call_args
            assert "USDC" in call_args
            assert "25" in call_args  # 25% drift


class TestRejectionCounter:
    """Phase 9: Rejection counter tests."""

    def test_fallback_stats_dataclass(self) -> None:
        """Step 9.1: FallbackStats dataclass has rejection_counts and fallback_usage."""
        from parsertang.static_withdrawal_fees import FallbackStats

        stats = FallbackStats()

        assert hasattr(stats, "rejection_counts")
        assert hasattr(stats, "fallback_usage")
        assert isinstance(stats.rejection_counts, dict)
        assert isinstance(stats.fallback_usage, dict)

    def test_record_rejection_increments_counter(self) -> None:
        """Step 9.3: record_rejection() increments rejection counter."""
        from parsertang.static_withdrawal_fees import FallbackStats

        stats = FallbackStats()

        stats.record_rejection("okx", "USDC", "ARB")
        assert stats.rejection_counts[("okx", "USDC", "ARB")] == 1

        stats.record_rejection("okx", "USDC", "ARB")
        assert stats.rejection_counts[("okx", "USDC", "ARB")] == 2

        stats.record_rejection("bybit", "USDT", "TRC20")
        assert stats.rejection_counts[("bybit", "USDT", "TRC20")] == 1

    def test_record_fallback_usage_increments_counter(self) -> None:
        """Step 9.3: record_fallback_usage() increments fallback usage counter."""
        from parsertang.static_withdrawal_fees import FallbackStats

        stats = FallbackStats()

        stats.record_fallback_usage("okx", "USDC", "ARB")
        assert stats.fallback_usage[("okx", "USDC", "ARB")] == 1

        stats.record_fallback_usage("okx", "USDC", "ARB")
        stats.record_fallback_usage("okx", "USDC", "ARB")
        assert stats.fallback_usage[("okx", "USDC", "ARB")] == 3


class TestDailyReport:
    """Phase 10: Daily report tests."""

    def test_format_daily_fee_report_generates_text(self) -> None:
        """Step 10.1: format_daily_fee_report() generates report text."""
        from parsertang.static_withdrawal_fees import (
            FallbackStats,
            format_daily_fee_report,
        )

        stats = FallbackStats()
        stats.record_rejection("okx", "USDC", "ARB")
        stats.record_rejection("okx", "USDC", "ARB")
        stats.record_fallback_usage("okx", "USDC", "ARB")
        stats.record_fallback_usage("bybit", "USDT", "TRC20")

        report = format_daily_fee_report(stats)

        # Report should contain rejection info
        assert "okx" in report
        assert "USDC" in report
        assert "ARB" in report
        assert "2" in report  # 2 rejections

        # Report should contain fallback usage info
        assert "bybit" in report or "USDT" in report

    def test_format_daily_fee_report_empty_stats(self) -> None:
        """Step 10.1: format_daily_fee_report() handles empty stats."""
        from parsertang.static_withdrawal_fees import (
            FallbackStats,
            format_daily_fee_report,
        )

        stats = FallbackStats()

        report = format_daily_fee_report(stats)

        assert "No rejections" in report or len(report) > 0

    def test_reset_daily_stats_clears_counters(self) -> None:
        """Step 10.3: reset_daily_stats() clears all counters."""
        from parsertang.static_withdrawal_fees import FallbackStats

        stats = FallbackStats()
        stats.record_rejection("okx", "USDC", "ARB")
        stats.record_fallback_usage("okx", "USDC", "ARB")

        # Verify data exists
        assert len(stats.rejection_counts) == 1
        assert len(stats.fallback_usage) == 1

        # Reset
        stats.reset_daily_stats()

        # Verify cleared
        assert len(stats.rejection_counts) == 0
        assert len(stats.fallback_usage) == 0


class TestE2EIntegration:
    """Phase 11: End-to-end integration tests."""

    def test_e2e_dynamic_fee_available_no_fallback(self) -> None:
        """Step 11.1: When dynamic fee is available, HIGH confidence, no fallback."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Cache has OKX USDC/ARB with dynamic fee
        manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {
                    "USDC": {
                        "ARB": 0.003
                    },  # Dynamic fee, different from fallback 0.004
                    "USDT": {"TRC20": 1.0},
                },
            },
            last_updated=time.time(),
        )

        with unittest.mock.patch(
            "parsertang.withdrawal_fees.get_fallback_fee"
        ) as mock_fallback:
            fee, confidence = manager.get_withdrawal_fee("okx", "USDC", "ARB")

            # Should use dynamic fee, not fallback
            assert fee == 0.003  # Dynamic, not fallback 0.004
            assert confidence == "HIGH"

            # Fallback should NOT have been called for fee lookup
            # (it's only called when cache miss occurs)
            mock_fallback.assert_not_called()

    def test_e2e_fallback_used_when_currency_missing(self) -> None:
        """Step 11.2: When currency missing from cache, MEDIUM confidence, fallback used."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Cache has OKX data but NOT USDC
        manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {
                    "USDT": {"TRC20": 1.0},  # Has USDT, not USDC
                },
            },
            last_updated=time.time(),
        )

        with unittest.mock.patch("parsertang.withdrawal_fees.logger") as mock_logger:
            fee, confidence = manager.get_withdrawal_fee("okx", "USDC", "ARB")

            # Should use fallback fee
            assert fee == 0.004  # From FALLBACK_FEES
            assert confidence == "MEDIUM"

            # Should log fallback usage
            mock_logger.info.assert_called()
            call_args = str(mock_logger.info.call_args)
            assert "FEE FALLBACK" in call_args

    def test_e2e_api_down_no_fallback(self) -> None:
        """Step 11.3: When API down (empty exchange cache), None returned, no fallback."""
        import time

        from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager

        manager = WithdrawalFeeManager(exchanges={})
        # Cache exists but OKX is empty (API down scenario)
        manager.cache = WithdrawalFeeCache(
            fees={
                "okx": {},  # Empty - API down
                "bybit": {"USDT": {"TRC20": 1.0}},  # Other exchange works
            },
            last_updated=time.time(),
        )

        with unittest.mock.patch(
            "parsertang.withdrawal_fees.get_fallback_fee"
        ) as mock_fallback:
            fee, confidence = manager.get_withdrawal_fee("okx", "USDC", "ARB")

            # Should return None, not fallback (we don't know if API is down or fee doesn't exist)
            assert fee is None
            assert confidence == "HIGH"

            # Fallback should NOT be used when API is suspected to be down
            mock_fallback.assert_not_called()
