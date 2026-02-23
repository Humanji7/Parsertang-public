"""Tests for logging configuration pipeline (Protocol 0001 - Step 3)."""

import logging
import time

import pytest

from src.parsertang.logging_conf import SamplingFilter, SensitiveDataFilter


class TestSamplingFilterCriticalPrefixes:
    """Test that critical prefixes bypass all sampling."""

    def test_arb_prefix_bypasses_sampling(self):
        """ARB prefix always passes through."""
        filter_ = SamplingFilter(ratio=1000)  # Very high ratio

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="ARB OK spread=0.5%",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_cycle_prefix_bypasses_sampling(self):
        """CYCLE prefix always passes through."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="CYCLE LEG1 started",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_leg_prefix_bypasses_sampling(self):
        """LEG prefix always passes through."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="LEG1 buy order placed",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_error_prefix_bypasses_sampling(self):
        """ERROR in message bypasses sampling."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="ERROR connection failed",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_all_critical_prefixes(self):
        """All critical prefixes bypass sampling."""
        filter_ = SamplingFilter(ratio=1000)

        for prefix in ["ARB", "CYCLE", "LEG", "ERROR", "CRITICAL"]:
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"{prefix} test message",
                args=(),
                exc_info=None,
            )
            assert filter_.filter(record) is True, f"{prefix} should bypass"

    def test_truth_prefix_bypasses_sampling(self):
        """TRUTH prefix always passes through."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="TRUTH OK | sample",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_alert_suppressed_prefix_bypasses_sampling(self):
        """ALERT SUPPRESSED prefix always passes through."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="ALERT SUPPRESSED | truth_gate=off",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True


class TestSamplingFilterRatio:
    """Test counter-based sampling ratio."""

    def test_ratio_10_samples_approximately_10_percent(self):
        """With ratio=10, approximately 10% of messages pass."""
        filter_ = SamplingFilter(ratio=10)

        passed = 0
        for i in range(100):
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"WS message {i}",
                args=(),
                exc_info=None,
            )
            if filter_.filter(record):
                passed += 1

        # Should be exactly 10 (every 10th message passes)
        assert passed == 10

    def test_ratio_1_means_no_sampling(self):
        """With ratio=1, all messages pass (no sampling)."""
        filter_ = SamplingFilter(ratio=1)

        passed = 0
        for i in range(50):
            record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"WS message {i}",
                args=(),
                exc_info=None,
            )
            if filter_.filter(record):
                passed += 1

        assert passed == 50

    def test_different_prefixes_have_separate_counters(self):
        """Each prefix has its own counter."""
        filter_ = SamplingFilter(ratio=5)

        # First message of each prefix should pass (counter becomes 1, 1%5 != 0)
        # Actually 5th message passes (counter becomes 5, 5%5 == 0)
        ws_passed = 0
        ob_passed = 0

        for i in range(20):
            ws_record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"WS msg {i}",
                args=(),
                exc_info=None,
            )
            ob_record = logging.LogRecord(
                name="test",
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg=f"OB update {i}",
                args=(),
                exc_info=None,
            )
            if filter_.filter(ws_record):
                ws_passed += 1
            if filter_.filter(ob_record):
                ob_passed += 1

        # Each should pass 4 times (5, 10, 15, 20)
        assert ws_passed == 4
        assert ob_passed == 4


class TestSamplingFilterSuppression:
    """Test prefix suppression list."""

    def test_suppressed_prefixes_blocked(self):
        """Suppressed prefixes are completely blocked."""
        filter_ = SamplingFilter(suppress={"TICK", "HEARTBEAT"})

        tick_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="TICK update price=100",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(tick_record) is False

        heartbeat_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="HEARTBEAT ping",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(heartbeat_record) is False

    def test_non_suppressed_prefixes_pass(self):
        """Non-suppressed prefixes are not blocked."""
        filter_ = SamplingFilter(ratio=1, suppress={"TICK"})

        ws_record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS connected",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(ws_record) is True


class TestSamplingFilterWarningLevel:
    """Test that WARNING and above bypass sampling."""

    def test_warning_level_bypasses_sampling(self):
        """WARNING level messages bypass sampling."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="WS connection slow",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_error_level_bypasses_sampling(self):
        """ERROR level messages bypass sampling."""
        filter_ = SamplingFilter(ratio=1000)

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="WS connection failed",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True


class TestSamplingFilterTimeInterval:
    """Test time-based sampling interval."""

    def test_first_message_passes(self):
        """First message always passes."""
        filter_ = SamplingFilter(ratio=1, interval=1.0)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS msg",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_immediate_second_message_blocked(self):
        """Second message within interval is blocked."""
        filter_ = SamplingFilter(ratio=1, interval=0.5)

        record1 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS msg1",
            args=(),
            exc_info=None,
        )
        filter_.filter(record1)  # First passes

        record2 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS msg2",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record2) is False  # Too soon

    def test_message_after_interval_passes(self):
        """Message after interval has passed goes through."""
        filter_ = SamplingFilter(ratio=1, interval=0.1)

        record1 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS msg1",
            args=(),
            exc_info=None,
        )
        filter_.filter(record1)

        time.sleep(0.15)  # Wait for interval

        record2 = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="WS msg2",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record2) is True


class TestSensitiveDataFilter:
    """Test sensitive data masking."""

    def test_masks_token_in_url(self):
        """Token in URL is masked."""
        filter_ = SensitiveDataFilter()

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Connecting to wss://api.example.com?token=secret123abc",
            args=(),
            exc_info=None,
        )
        filter_.filter(record)

        assert "secret123abc" not in record.msg
        assert "***MASKED***" in record.msg

    def test_masks_api_key(self):
        """API key is masked."""
        filter_ = SensitiveDataFilter()

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="api_key=abcdefghij1234567890abcd",
            args=(),
            exc_info=None,
        )
        filter_.filter(record)

        assert "abcdefghij1234567890abcd" not in record.msg
        assert "***MASKED***" in record.msg


class TestSamplingFilterEdgeCases:
    """Test edge cases."""

    def test_empty_message(self):
        """Empty message passes through."""
        filter_ = SamplingFilter(ratio=10)

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True

    def test_ratio_zero_treated_as_one(self):
        """Ratio of 0 is treated as 1 (no sampling)."""
        filter_ = SamplingFilter(ratio=0)  # Should become 1

        assert filter_.ratio == 1


# ============================================================================
# Configuration Validation Tests
# ============================================================================


class TestLoggingConfigurationValidation:
    """Test Pydantic settings validation for logging config."""

    def test_default_logging_settings(self):
        """Test default logging configuration values."""
        from src.parsertang.config import Settings

        settings = Settings()
        assert settings.log_level_console == "WARNING"
        assert settings.log_level_file == "INFO"
        assert settings.log_max_bytes == 104857600  # 100 MB
        assert settings.log_backup_count == 5
        assert settings.log_sample_ratio == 10
        assert settings.log_sample_interval_seconds == 0.0
        assert settings.log_suppress_prefixes == ""

    def test_invalid_log_level_raises_error(self):
        """Test that invalid log levels raise ValidationError."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="Invalid log level"):
            Settings(log_level_console="TRACE")

        with pytest.raises(ValidationError, match="Invalid log level"):
            Settings(log_level_file="VERBOSE")

    def test_log_level_case_normalization(self):
        """Test that log levels are normalized to uppercase."""
        from src.parsertang.config import Settings

        settings = Settings(log_level_console="debug", log_level_file="warning")
        assert settings.log_level_console == "DEBUG"
        assert settings.log_level_file == "WARNING"

    def test_max_bytes_too_small_raises_error(self):
        """Test LOG_MAX_BYTES minimum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_max_bytes must be between"):
            Settings(log_max_bytes=1000)  # < 1 MB

    def test_max_bytes_too_large_raises_error(self):
        """Test LOG_MAX_BYTES maximum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_max_bytes must be between"):
            Settings(log_max_bytes=2_000_000_000)  # > 1 GB

    def test_max_bytes_valid_range(self):
        """Test LOG_MAX_BYTES accepts valid values."""
        from src.parsertang.config import Settings

        settings = Settings(log_max_bytes=10_485_760)  # 10 MB
        assert settings.log_max_bytes == 10_485_760

    def test_backup_count_negative_raises_error(self):
        """Test LOG_BACKUP_COUNT minimum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_backup_count must be between"):
            Settings(log_backup_count=-1)

    def test_backup_count_too_large_raises_error(self):
        """Test LOG_BACKUP_COUNT maximum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_backup_count must be between"):
            Settings(log_backup_count=50)

    def test_sample_ratio_zero_raises_error(self):
        """Test LOG_SAMPLE_RATIO minimum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_sample_ratio must be between"):
            Settings(log_sample_ratio=0)

    def test_sample_ratio_too_large_raises_error(self):
        """Test LOG_SAMPLE_RATIO maximum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(ValidationError, match="log_sample_ratio must be between"):
            Settings(log_sample_ratio=2000)

    def test_sample_interval_negative_raises_error(self):
        """Test LOG_SAMPLE_INTERVAL_SECONDS minimum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(
            ValidationError, match="log_sample_interval_seconds must be between"
        ):
            Settings(log_sample_interval_seconds=-1.0)

    def test_sample_interval_too_large_raises_error(self):
        """Test LOG_SAMPLE_INTERVAL_SECONDS maximum bound."""
        from pydantic import ValidationError
        from src.parsertang.config import Settings

        with pytest.raises(
            ValidationError, match="log_sample_interval_seconds must be between"
        ):
            Settings(log_sample_interval_seconds=100.0)

    def test_suppress_prefixes_parsing(self):
        """Test parsing of comma-separated suppress list."""
        from src.parsertang.config import Settings

        settings = Settings(log_suppress_prefixes="TICK, HEARTBEAT, PING")
        prefixes = settings.get_suppress_prefixes()
        assert prefixes == ["TICK", "HEARTBEAT", "PING"]

    def test_empty_suppress_prefixes(self):
        """Test empty suppress prefixes list."""
        from src.parsertang.config import Settings

        settings = Settings(log_suppress_prefixes="")
        assert settings.get_suppress_prefixes() == []

    def test_whitespace_only_suppress_prefixes(self):
        """Test whitespace-only suppress prefixes."""
        from src.parsertang.config import Settings

        settings = Settings(log_suppress_prefixes="  ,  ,  ")
        assert settings.get_suppress_prefixes() == []
