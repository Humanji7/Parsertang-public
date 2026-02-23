"""Unit tests for ExchangeHealthMonitor circuit breaker implementation."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from parsertang.health_monitor import (
    CircuitOpenError,
    CircuitState,
    ExchangeHealthMonitor,
    is_transient_failure,
    v2_sla_report_task,
)


class MockSettings:
    """Mock settings for testing."""

    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_seconds: int = 60
    circuit_half_open_max_calls: int = 1


@pytest.fixture
def settings():
    """Create mock settings."""
    return MockSettings()


@pytest.fixture
def monitor(settings):
    """Create ExchangeHealthMonitor with mock settings."""
    return ExchangeHealthMonitor(settings)


# --- State Tests ---


def test_initial_state_is_closed(monitor):
    """New circuit should be in CLOSED state."""
    state = monitor.get_state("bybit")
    assert state == CircuitState.CLOSED


def test_is_available_when_closed(monitor):
    """Exchange should be available when circuit is CLOSED."""
    assert monitor.is_available("bybit") is True


# --- Failure Classification Tests ---


def test_is_transient_failure_connection_error():
    """ConnectionError should be classified as transient."""
    assert is_transient_failure(ConnectionError("connection refused")) is True


def test_is_transient_failure_timeout():
    """TimeoutError should be classified as transient."""
    assert is_transient_failure(TimeoutError("timed out")) is True


def test_is_transient_failure_503():
    """503 Service Unavailable should be classified as transient."""
    error = Exception("HTTP 503 Service Unavailable")
    assert is_transient_failure(error) is True


def test_is_transient_failure_rate_limit():
    """Rate limit errors should be classified as transient."""
    error = Exception("rate limit exceeded")
    assert is_transient_failure(error) is True


def test_is_not_transient_failure_value_error():
    """ValueError should NOT be classified as transient."""
    assert is_transient_failure(ValueError("invalid symbol")) is False


def test_is_not_transient_failure_400():
    """400 Bad Request should NOT be classified as transient."""
    error = Exception("HTTP 400 Bad Request")
    assert is_transient_failure(error) is False


# --- Success/Failure Recording Tests ---


def test_success_resets_counter(monitor):
    """record_success should reset failure_count."""
    # Simulate some failures first
    for _ in range(2):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    circuit = monitor._circuits["bybit"]
    assert circuit.failure_count == 2

    # Success should reset
    monitor.record_success("bybit")
    assert circuit.failure_count == 0


def test_failures_increment_counter(monitor):
    """record_failure should increment failure_count."""
    monitor.record_failure("bybit", TimeoutError("timeout"))
    assert monitor._circuits["bybit"].failure_count == 1

    monitor.record_failure("bybit", ConnectionError("refused"))
    assert monitor._circuits["bybit"].failure_count == 2


def test_non_transient_failure_ignored(monitor):
    """Non-transient errors should not increment counter."""
    monitor.record_failure("bybit", ValueError("bad param"))
    # Should not have created a circuit with failures
    if "bybit" in monitor._circuits:
        assert monitor._circuits["bybit"].failure_count == 0


# --- State Transition Tests ---


def test_threshold_triggers_open(monitor):
    """Reaching failure threshold should transition to OPEN."""
    for _ in range(3):  # threshold is 3
        monitor.record_failure("bybit", TimeoutError("timeout"))

    assert monitor.get_state("bybit") == CircuitState.OPEN


def test_open_rejects_calls(monitor):
    """Exchange should not be available when circuit is OPEN."""
    # Trigger OPEN state
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    assert monitor.is_available("bybit") is False


def test_timeout_allows_half_open(monitor):
    """After recovery timeout, exchange should be available (for probe)."""
    # Trigger OPEN state
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    # Manually set last_state_change to past
    circuit = monitor._circuits["bybit"]
    circuit.last_state_change = datetime.utcnow() - timedelta(seconds=120)

    # Should now be available for probe
    assert monitor.is_available("bybit") is True


def test_half_open_success_closes(monitor):
    """Success in HALF_OPEN should transition to CLOSED."""
    # Trigger OPEN state
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    # Fast-forward time
    circuit = monitor._circuits["bybit"]
    circuit.last_state_change = datetime.utcnow() - timedelta(seconds=120)

    # Acquire probe and succeed
    monitor.acquire_probe("bybit")
    monitor.record_success("bybit")

    assert monitor.get_state("bybit") == CircuitState.CLOSED


def test_half_open_failure_reopens(monitor):
    """Failure in HALF_OPEN should transition back to OPEN."""
    # Trigger OPEN state
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    # Transition to HALF_OPEN
    circuit = monitor._circuits["bybit"]
    circuit.state = CircuitState.HALF_OPEN
    circuit.probe_in_progress = True

    # Probe fails
    monitor.record_failure("bybit", TimeoutError("still failing"))

    assert monitor.get_state("bybit") == CircuitState.OPEN


# --- Probe Gating Tests ---


def test_acquire_probe_sets_flag(monitor):
    """acquire_probe should set probe_in_progress flag."""
    # Trigger OPEN then transition to HALF_OPEN
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    circuit = monitor._circuits["bybit"]
    circuit.last_state_change = datetime.utcnow() - timedelta(seconds=120)

    result = monitor.acquire_probe("bybit")
    assert result is True
    assert circuit.probe_in_progress is True


def test_second_probe_blocked(monitor):
    """Second probe should be blocked when one is in progress."""
    # Setup HALF_OPEN with probe in progress
    for _ in range(3):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    circuit = monitor._circuits["bybit"]
    circuit.last_state_change = datetime.utcnow() - timedelta(seconds=120)

    # First probe
    monitor.acquire_probe("bybit")

    # Second probe should be blocked
    result = monitor.acquire_probe("bybit")
    assert result is False


# --- Disabled Circuit Breaker Tests ---


def test_disabled_always_available():
    """When disabled, all exchanges should always be available."""
    settings = MockSettings()
    settings.circuit_breaker_enabled = False
    monitor = ExchangeHealthMonitor(settings)

    # Even with failures, should be available
    for _ in range(10):
        monitor.record_failure("bybit", TimeoutError("timeout"))

    assert monitor.is_available("bybit") is True


@pytest.mark.asyncio
async def test_v2_sla_report_sent():
    send_tech = AsyncMock()

    def get_metrics():
        return 0.96, 0.85

    await v2_sla_report_task(
        get_metrics_fn=get_metrics,
        send_fn=send_tech,
        interval_seconds=0,
        run_once=True,
    )

    send_tech.assert_awaited_once()
    report_text = send_tech.await_args.args[0]
    assert "96%" in report_text
    assert "0.85" in report_text


# --- Health Summary Tests ---


def test_get_health_summary(monitor):
    """Health summary should return status for all circuits."""
    monitor.record_failure("bybit", TimeoutError("timeout"))
    monitor.record_failure("okx", ConnectionError("refused"))

    summary = monitor.get_health_summary()

    assert "bybit" in summary
    assert "okx" in summary
    assert summary["bybit"]["state"] == "closed"
    assert summary["bybit"]["failure_count"] == 1


# --- Logging Tests ---


def test_state_transition_logging(monitor, caplog):
    """State transitions should be logged."""
    import logging

    with caplog.at_level(logging.INFO):
        for _ in range(3):
            monitor.record_failure("bybit", TimeoutError("timeout"))

    # Should have logged the OPEN transition
    assert "CIRCUIT_OPEN" in caplog.text
    assert "bybit" in caplog.text


# --- CircuitOpenError Tests ---


def test_circuit_open_error():
    """CircuitOpenError should contain exchange and retry info."""
    error = CircuitOpenError("bybit", 120)

    assert error.exchange_id == "bybit"
    assert error.retry_after == 120
    assert "bybit" in str(error)
    assert "120" in str(error)
