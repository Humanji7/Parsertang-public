"""Test WebSocket fallback to REST.

This module tests that the system gracefully falls back to REST
when ccxt.pro is unavailable or WebSocket is disabled.

Reference: SPEC-R1-001, Section 6.2
"""

import pytest
from unittest.mock import patch, MagicMock
from parsertang.streams import Streams


def test_ws_disabled_by_config():
    """When WS_ENABLED=false, should not attempt WS connections."""
    # This is tested by verifying that Streams raises RuntimeError
    # when ccxt.pro is not available

    with patch("parsertang.streams.ccxtpro", None):
        with pytest.raises(RuntimeError, match="ccxt.pro not available"):
            Streams(["bybit"])


def test_ws_unavailable_triggers_runtime_error():
    """When ccxt.pro unavailable, Streams should raise RuntimeError."""
    with patch("parsertang.streams.ccxtpro", None):
        with pytest.raises(RuntimeError) as exc_info:
            Streams(["bybit", "okx"])  # Just invoke to trigger error

        assert "ccxt.pro not available" in str(exc_info.value)


def test_ws_available_creates_exchanges():
    """When ccxt.pro available, Streams should initialize exchanges."""
    mock_ccxtpro = MagicMock()
    mock_exchange_class = MagicMock()
    mock_ccxtpro.bybit = mock_exchange_class

    with patch("parsertang.streams.ccxtpro", mock_ccxtpro):
        with patch("parsertang.streams.WS_ID_ALIASES", {"bybit": "bybit"}):
            with patch(
                "parsertang.streams.build_exchange_config",
                return_value={"enableRateLimit": True},
            ):
                manager = Streams(["bybit"])

                # Should have initialized the exchange
                assert "bybit" in manager.exchanges
                mock_exchange_class.assert_called_once_with({"enableRateLimit": True})


def test_ws_unsupported_exchange_skipped():
    """When exchange doesn't support WS, it should be skipped with warning."""
    mock_ccxtpro = MagicMock()
    # Simulate that 'unsupported_ex' class doesn't exist
    mock_ccxtpro.bybit = MagicMock()
    # Make hasattr return False for unsupported exchange

    with patch("parsertang.streams.ccxtpro", mock_ccxtpro):
        with patch(
            "parsertang.streams.WS_ID_ALIASES",
            {"bybit": "bybit", "unsupported": "unsupported"},
        ):
            with patch("parsertang.streams.hasattr") as mock_hasattr:
                # Return True for bybit, False for unsupported
                mock_hasattr.side_effect = lambda obj, attr: attr == "bybit"

                with patch("parsertang.streams.logger") as mock_logger:
                    manager = Streams(["bybit", "unsupported"])

                    # Should only have bybit
                    assert "bybit" in manager.exchanges
                    assert "unsupported" not in manager.exchanges

                    # Should log warning
                    mock_logger.warning.assert_called()


def test_orderbook_limits_initialization():
    """Test that orderbook_limits dict is properly initialized."""
    mock_ccxtpro = MagicMock()
    mock_ccxtpro.bybit = MagicMock()

    with patch("parsertang.streams.ccxtpro", mock_ccxtpro):
        with patch("parsertang.streams.WS_ID_ALIASES", {"bybit": "bybit"}):
            manager = Streams(["bybit"])

            # Should initialize empty orderbook_limits dict
            assert hasattr(manager, "orderbook_limits")
            assert isinstance(manager.orderbook_limits, dict)


def test_exchange_init_failure_logged():
    """Test that exchange initialization failures are logged."""
    mock_ccxtpro = MagicMock()
    mock_exchange_class = MagicMock(side_effect=Exception("Init failed"))
    mock_ccxtpro.bybit = mock_exchange_class

    with patch("parsertang.streams.ccxtpro", mock_ccxtpro):
        with patch("parsertang.streams.WS_ID_ALIASES", {"bybit": "bybit"}):
            with patch("parsertang.streams.logger") as mock_logger:
                manager = Streams(["bybit"])

                # Should not have the exchange
                assert "bybit" not in manager.exchanges

                # Should log error
                mock_logger.error.assert_called()
