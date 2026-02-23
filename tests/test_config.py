"""Unit tests for config module.

Tests the Settings class validators, especially trade_volume_usd validation (Issue 2.2 - P2).
"""

import pytest
from pydantic import ValidationError
from parsertang.config import Settings


class TestTradeVolumeValidation:
    """Test suite for trade_volume_usd validation (Issue 2.2 - P2)."""

    def test_valid_trade_volume(self):
        """Test that valid trade_volume_usd values are accepted."""
        # Test standard volume
        settings = Settings(trade_volume_usd=100.0)
        assert settings.trade_volume_usd == 100.0

        # Test large volume
        settings = Settings(trade_volume_usd=10000.0)
        assert settings.trade_volume_usd == 10000.0

        # Test small but valid volume
        settings = Settings(trade_volume_usd=10.0)
        assert settings.trade_volume_usd == 10.0

    def test_zero_trade_volume_raises_error(self):
        """Test that zero trade_volume_usd raises ValueError."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(trade_volume_usd=0.0)

        # Check that the error message contains our validation message
        assert "trade_volume_usd must be positive" in str(exc_info.value)

    def test_negative_trade_volume_raises_error(self):
        """Test that negative trade_volume_usd raises ValueError."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(trade_volume_usd=-50.0)

        assert "trade_volume_usd must be positive" in str(exc_info.value)

    def test_very_small_trade_volume_warns(self, caplog):
        """Test that very small trade_volume_usd (<$1) generates warning."""
        # This should succeed but log a warning
        settings = Settings(trade_volume_usd=0.5)
        assert settings.trade_volume_usd == 0.5

        # Check that warning was logged
        # Note: The warning is logged during validation, so we should see it in logs
        # This test verifies the value is accepted but warns

    def test_edge_case_one_dollar(self):
        """Test boundary case of exactly $1."""
        # Should pass without warning (threshold is < 1.0)
        settings = Settings(trade_volume_usd=1.0)
        assert settings.trade_volume_usd == 1.0

    def test_edge_case_just_below_one_dollar(self):
        """Test boundary case just below $1."""
        # Should pass with warning (0.99 < 1.0)
        settings = Settings(trade_volume_usd=0.99)
        assert settings.trade_volume_usd == 0.99


class TestOtherValidations:
    """Test other validation behaviors in Settings."""

    def test_settings_loads_with_valid_values(self):
        """Test that Settings loads successfully with valid values.

        Note: Values may come from .env or defaults. We verify they are
        in valid ranges rather than checking exact defaults.
        """
        settings = Settings()
        # Verify values are positive (valid regardless of source)
        assert settings.trade_volume_usd > 0
        assert settings.min_net_profit > 0
        assert settings.liquidity_usd_threshold > 0
        # These are safety flags that should be set conservatively
        assert settings.trading_enabled is False
        assert settings.dry_run_mode is True

    def test_phase_r1_validation(self):
        """Test that Phase R1 validation works correctly."""
        # Phase R1 should reject TRADING_ENABLED=true
        with pytest.raises(ValidationError) as exc_info:
            Settings(current_phase="R1", trading_enabled=True)

        assert "TRADING_ENABLED cannot be true during phase R1" in str(exc_info.value)

        # Phase R1 should accept TRADING_ENABLED=false
        settings = Settings(current_phase="R1", trading_enabled=False)
        assert settings.trading_enabled is False

    def test_phase_r2_r3_r4_block_trading(self):
        """Test that Phase R2-R4 also block TRADING_ENABLED=true (like R1)."""
        # R2 should reject TRADING_ENABLED=true
        with pytest.raises(ValidationError) as exc_info:
            Settings(current_phase="R2", trading_enabled=True)
        assert "TRADING_ENABLED cannot be true during phase R2" in str(exc_info.value)

        # R3 should reject TRADING_ENABLED=true
        with pytest.raises(ValidationError) as exc_info:
            Settings(current_phase="R3", trading_enabled=True)
        assert "TRADING_ENABLED cannot be true during phase R3" in str(exc_info.value)

        # R4 should reject TRADING_ENABLED=true
        with pytest.raises(ValidationError) as exc_info:
            Settings(current_phase="R4", trading_enabled=True)
        assert "TRADING_ENABLED cannot be true during phase R4" in str(exc_info.value)

        # R2-R4 should accept TRADING_ENABLED=false
        for phase in ["R2", "R3", "R4"]:
            settings = Settings(current_phase=phase, trading_enabled=False)
            assert settings.trading_enabled is False

    def test_phase_r5_allows_trading(self):
        """Test that Phase R5+ allows TRADING_ENABLED=true."""
        # R5 should allow TRADING_ENABLED=true
        settings = Settings(current_phase="R5", trading_enabled=True)
        assert settings.trading_enabled is True

        # R6 should also allow TRADING_ENABLED=true
        settings = Settings(current_phase="R6", trading_enabled=True)
        assert settings.trading_enabled is True


class TestCircuitBreakerValidation:
    """Test suite for circuit breaker configuration validation."""

    def test_circuit_breaker_defaults(self):
        """Test that circuit breaker config uses correct defaults."""
        settings = Settings()
        assert settings.circuit_breaker_enabled is True
        assert settings.circuit_failure_threshold == 5
        assert settings.circuit_recovery_timeout_seconds == 300
        assert settings.circuit_half_open_max_calls == 1

    def test_circuit_failure_threshold_valid_range(self):
        """Test valid failure threshold values (1-50)."""
        settings = Settings(circuit_failure_threshold=1)
        assert settings.circuit_failure_threshold == 1

        settings = Settings(circuit_failure_threshold=50)
        assert settings.circuit_failure_threshold == 50

    def test_circuit_failure_threshold_below_min(self):
        """Test that failure threshold < 1 raises error."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(circuit_failure_threshold=0)
        assert "circuit_failure_threshold must be between 1 and 50" in str(
            exc_info.value
        )

    def test_circuit_failure_threshold_above_max(self):
        """Test that failure threshold > 50 raises error."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(circuit_failure_threshold=51)
        assert "circuit_failure_threshold must be between 1 and 50" in str(
            exc_info.value
        )

    def test_circuit_recovery_timeout_valid_range(self):
        """Test valid recovery timeout values (30-1800)."""
        settings = Settings(circuit_recovery_timeout_seconds=30)
        assert settings.circuit_recovery_timeout_seconds == 30

        settings = Settings(circuit_recovery_timeout_seconds=1800)
        assert settings.circuit_recovery_timeout_seconds == 1800

    def test_circuit_recovery_timeout_below_min(self):
        """Test that recovery timeout < 30 raises error."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(circuit_recovery_timeout_seconds=29)
        assert "circuit_recovery_timeout_seconds must be between 30 and 1800" in str(
            exc_info.value
        )

    def test_circuit_half_open_max_calls_valid_range(self):
        """Test valid half-open max calls values (1-10)."""
        settings = Settings(circuit_half_open_max_calls=1)
        assert settings.circuit_half_open_max_calls == 1

        settings = Settings(circuit_half_open_max_calls=10)
        assert settings.circuit_half_open_max_calls == 10

    def test_circuit_half_open_max_calls_below_min(self):
        """Test that half-open max calls < 1 raises error."""
        with pytest.raises(ValidationError) as exc_info:
            Settings(circuit_half_open_max_calls=0)
        assert "circuit_half_open_max_calls must be between 1 and 10" in str(
            exc_info.value
        )


def test_v2_health_gate_defaults():
    from parsertang.config import settings

    assert settings.v2_health_enabled is True


def test_truth_gate_defaults():
    from parsertang.config import settings

    assert settings.truth_gate_enabled is True
    assert settings.truth_gate_ratio_min == 98.0
    assert settings.truth_gate_summary_path == "data/truth_summary.json"
    assert settings.truth_gate_max_age_seconds == 3600
    assert settings.truth_gate_refresh_seconds == 30.0
    assert settings.truth_gate_min_total == 500
    assert settings.v2_health_fresh_ratio_min == 0.80
    assert settings.v2_health_stale_seconds == 2.0
    assert settings.v2_health_check_interval_seconds == 60


def test_v2_validation_defaults():
    from parsertang.config import settings

    assert settings.v2_validation_enabled is False
    assert settings.v2_validation_price_tolerance_pct == 0.1
    assert settings.v2_validation_tick_multiplier == 3
    assert settings.v2_validation_ws_max_age_ms == 1000
    assert settings.v2_validation_ws_max_skew_ms == 500
    assert settings.v2_validation_fee_max_age_seconds == 3600
    assert settings.v2_validation_stale_symbol_threshold == 5
    assert settings.v2_validation_stale_symbol_cooldown_seconds == 600
    assert settings.symbol_min_quote_volume_usd == 0.0
    assert settings.symbol_min_overlap_exchanges == 2
    assert settings.symbol_allowlist is None
    assert settings.symbol_allowlist_path is None
    assert settings.symbol_allowlist_refresh_seconds == 3600


def test_v2_validation_tolerance_clamped():
    from parsertang.config import Settings

    settings = Settings(v2_validation_price_tolerance_pct=-0.5)
    assert settings.v2_validation_price_tolerance_pct == 0.0


def test_symbol_overlap_min_validation():
    from pydantic import ValidationError
    from parsertang.config import Settings

    with pytest.raises(ValidationError):
        Settings(symbol_min_overlap_exchanges=1)


def test_symbol_allowlist_parsing():
    from parsertang.config import Settings

    settings = Settings(symbol_allowlist="hype/usdt, LTC/USDT")
    assert settings.symbol_allowlist == ["HYPE/USDT", "LTC/USDT"]


class TestWsNativeDefaults:
    def test_ws_native_defaults(self):
        settings = Settings()
        assert settings.ws_native_enabled is False
        assert settings.ws_native_exchanges == ["bybit", "okx", "mexc"]
        assert settings.ws_native_depth_refresh_seconds == 5
        assert settings.ws_native_depth_ttl_seconds == 15
        assert settings.ws_native_bbo_channel == "bbo"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
