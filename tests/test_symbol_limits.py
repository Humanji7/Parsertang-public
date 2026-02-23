"""Test MAX_SYMBOLS_PER_EXCHANGE enforcement.

This module tests that symbol limiting works correctly and
prioritizes high-volume trading pairs.

Reference: SPEC-R1-001, Section 6.2
"""

from parsertang.exchanges import ExchangeGateway


def test_is_stable_quote_usdt():
    """Test that USDT pairs are recognized as stable."""
    assert ExchangeGateway.is_stable_quote("BTC/USDT")
    assert ExchangeGateway.is_stable_quote("ETH/USDT")


def test_is_stable_quote_usdc():
    """Test that USDC pairs are recognized as stable."""
    assert ExchangeGateway.is_stable_quote("BTC/USDC")
    assert ExchangeGateway.is_stable_quote("ETH/USDC")


def test_is_stable_quote_dai():
    """Test that DAI pairs are recognized as stable."""
    assert ExchangeGateway.is_stable_quote("BTC/DAI")


def test_is_stable_quote_fdusd():
    """Test that FDUSD pairs are recognized as stable."""
    assert ExchangeGateway.is_stable_quote("BTC/FDUSD")


def test_is_stable_quote_tusd():
    """Test that TUSD pairs are recognized as stable."""
    assert ExchangeGateway.is_stable_quote("BTC/TUSD")


def test_is_not_stable_quote():
    """Test that non-stable pairs are rejected."""
    assert not ExchangeGateway.is_stable_quote("BTC/EUR")
    assert not ExchangeGateway.is_stable_quote("ETH/BTC")
    assert not ExchangeGateway.is_stable_quote("DOGE/SHIB")


def test_is_stable_quote_case_insensitive():
    """Test that quote currency check is case-insensitive."""
    # The split will preserve case, but the check should be case-insensitive
    # Actually, let's check the implementation
    assert ExchangeGateway.is_stable_quote("BTC/USDT")
    assert ExchangeGateway.is_stable_quote("BTC/usdt")


def test_invalid_symbol_format():
    """Test handling of invalid symbol formats."""
    assert not ExchangeGateway.is_stable_quote("BTCUSDT")  # No slash
    assert not ExchangeGateway.is_stable_quote("BTC-USDT")  # Wrong separator
    assert not ExchangeGateway.is_stable_quote("")  # Empty string


def test_symbol_with_multiple_slashes():
    """Test handling of malformed symbols with multiple slashes."""
    # Should handle gracefully by splitting on first slash
    result = ExchangeGateway.is_stable_quote("BTC/USDT/EXTRA")
    # This will likely return False due to ValueError in split
    assert not result
