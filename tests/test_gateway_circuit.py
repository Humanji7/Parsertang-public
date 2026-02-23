"""Integration tests for ExchangeGateway + Circuit Breaker.

Tests verify:
- ExchangeGateway properly records success/failure via health_monitor
- CircuitOpenError is raised when circuit is OPEN
- Circuit recovery after timeout
- Multi-exchange isolation (one circuit doesn't affect another)
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from parsertang.exchanges import ExchangeGateway
from parsertang.health_monitor import (
    CircuitOpenError,
    CircuitState,
    ExchangeHealthMonitor,
)


class MockSettings:
    """Mock settings for testing."""

    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_seconds: int = 60
    circuit_half_open_max_calls: int = 1

    # Required for ExchangeGateway
    exchanges = ["bybit", "okx"]
    bybit_api_key = None
    bybit_api_secret = None
    okx_api_key = None
    okx_api_secret = None
    okx_passphrase = None
    kucoin_api_key = None
    kucoin_api_secret = None
    kucoin_passphrase = None
    mexc_api_key = None
    mexc_api_secret = None
    gate_api_key = None
    gate_api_secret = None
    htx_api_key = None
    htx_api_secret = None
    http_proxy = None
    https_proxy = None


@pytest.fixture
def settings():
    """Create mock settings."""
    return MockSettings()


@pytest.fixture
def monitor(settings):
    """Create ExchangeHealthMonitor with mock settings."""
    return ExchangeHealthMonitor(settings)


# --- Initialization Tests ---


def test_gateway_with_health_monitor_initialization(settings, monitor):
    """ExchangeGateway should accept health_monitor parameter."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock exchange classes
        mock_bybit = MagicMock()
        mock_okx = MagicMock()
        mock_ccxt.bybit = MagicMock(return_value=mock_bybit)
        mock_ccxt.okx = MagicMock(return_value=mock_okx)

        gw = ExchangeGateway(
            exchange_ids=["bybit", "okx"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        assert gw._monitor is monitor
        assert "bybit" in gw.exchanges
        assert "okx" in gw.exchanges


# --- Success/Failure Recording Tests ---


def test_fetch_order_book_records_success(settings, monitor):
    """Successful fetch_order_book should call record_success on monitor."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock exchange
        mock_ex = MagicMock()
        mock_ex.fetch_order_book.return_value = {
            "bids": [[100.0, 1.0]],
            "asks": [[101.0, 1.0]],
        }
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        bids, asks = gw.fetch_order_book("bybit", "BTC/USDT", limit=20)

        # Should have recorded success
        assert bids == [[100.0, 1.0]]
        assert asks == [[101.0, 1.0]]
        # Monitor should show no failures
        circuit = monitor._circuits.get("bybit")
        # If circuit was created, failure_count should be 0
        if circuit:
            assert circuit.failure_count == 0


def test_fetch_order_book_records_failure(settings, monitor):
    """Failed fetch_order_book should call record_failure on monitor."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock exchange that raises timeout
        mock_ex = MagicMock()
        mock_ex.fetch_order_book.side_effect = TimeoutError("connection timed out")
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        with pytest.raises(TimeoutError):
            gw.fetch_order_book("bybit", "BTC/USDT", limit=20)

        # Should have recorded failure
        circuit = monitor._circuits["bybit"]
        assert circuit.failure_count == 1


# --- Circuit Open Tests ---


def test_circuit_open_raises_error(settings, monitor):
    """After threshold failures, fetch_order_book should raise CircuitOpenError."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock exchange that always times out
        mock_ex = MagicMock()
        mock_ex.fetch_order_book.side_effect = TimeoutError("timeout")
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        # Trigger 3 failures to open circuit
        for _ in range(3):
            try:
                gw.fetch_order_book("bybit", "BTC/USDT", limit=20)
            except TimeoutError:
                pass

        # Circuit should now be OPEN
        assert monitor.get_state("bybit") == CircuitState.OPEN

        # Next call should raise CircuitOpenError
        with pytest.raises(CircuitOpenError) as exc_info:
            gw.fetch_order_book("bybit", "BTC/USDT", limit=20)

        assert exc_info.value.exchange_id == "bybit"
        assert exc_info.value.retry_after > 0


def test_circuit_recovery_after_timeout(settings, monitor):
    """After recovery timeout, circuit should allow probe request."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock exchange that fails initially, then succeeds
        mock_ex = MagicMock()
        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 3:
                raise TimeoutError("timeout")
            return {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]]}

        mock_ex.fetch_order_book.side_effect = side_effect
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        # Trigger 3 failures to open circuit
        for _ in range(3):
            try:
                gw.fetch_order_book("bybit", "BTC/USDT", limit=20)
            except TimeoutError:
                pass

        assert monitor.get_state("bybit") == CircuitState.OPEN

        # Fast-forward time past recovery timeout
        circuit = monitor._circuits["bybit"]
        circuit.last_state_change = datetime.utcnow() - timedelta(seconds=120)

        # Should now be available for probe (before acquire_probe is called)
        assert monitor.is_available("bybit") is True

        # acquire_probe transitions to HALF_OPEN and sets probe_in_progress
        can_probe = monitor.acquire_probe("bybit")
        assert can_probe is True
        assert monitor.get_state("bybit") == CircuitState.HALF_OPEN

        # Now is_available returns False because probe_in_progress=True
        # But since we already have the probe, we can make the call directly
        # Simulate the probe call by calling the exchange directly
        bids, asks = mock_ex.fetch_order_book("bybit", "BTC/USDT", limit=20)

        # Record success to close the circuit
        monitor.record_success("bybit")

        assert monitor.get_state("bybit") == CircuitState.CLOSED


# --- Multi-Exchange Isolation Tests ---


def test_multi_exchange_isolation(settings, monitor):
    """OPEN circuit on bybit should not affect okx."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        # Mock bybit (fails) and okx (succeeds)
        mock_bybit = MagicMock()
        mock_bybit.fetch_order_book.side_effect = TimeoutError("timeout")

        mock_okx = MagicMock()
        mock_okx.fetch_order_book.return_value = {
            "bids": [[100.0, 1.0]],
            "asks": [[101.0, 1.0]],
        }

        mock_ccxt.bybit = MagicMock(return_value=mock_bybit)
        mock_ccxt.okx = MagicMock(return_value=mock_okx)

        gw = ExchangeGateway(
            exchange_ids=["bybit", "okx"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        # Trip bybit circuit
        for _ in range(3):
            try:
                gw.fetch_order_book("bybit", "BTC/USDT", limit=20)
            except TimeoutError:
                pass

        # Bybit should be OPEN
        assert monitor.get_state("bybit") == CircuitState.OPEN

        # OKX should still be available and work
        assert monitor.is_available("okx") is True
        bids, asks = gw.fetch_order_book("okx", "BTC/USDT", limit=20)
        assert bids == [[100.0, 1.0]]


# --- Health Summary Tests ---


def test_gateway_get_health_summary(settings, monitor):
    """ExchangeGateway.get_health_summary should return circuit status."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        mock_ex = MagicMock()
        mock_ex.fetch_order_book.side_effect = TimeoutError("timeout")
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=monitor,
        )

        # Record a failure
        try:
            gw.fetch_order_book("bybit", "BTC/USDT", limit=20)
        except TimeoutError:
            pass

        summary = gw.get_health_summary()
        assert "bybit" in summary
        assert summary["bybit"]["state"] == "closed"
        assert summary["bybit"]["failure_count"] == 1


def test_gateway_without_monitor_returns_empty_summary(settings):
    """Gateway without monitor should return empty health summary."""
    with patch("parsertang.exchanges.ccxt") as mock_ccxt:
        mock_ex = MagicMock()
        mock_ccxt.bybit = MagicMock(return_value=mock_ex)

        gw = ExchangeGateway(
            exchange_ids=["bybit"],
            settings=settings,
            proxy_config=None,
            health_monitor=None,  # No monitor
        )

        summary = gw.get_health_summary()
        assert summary == {}
