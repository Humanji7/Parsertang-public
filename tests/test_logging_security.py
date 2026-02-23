"""
Tests for sensitive data masking in logs.
"""

import logging
from parsertang.logging_conf import SensitiveDataFilter


def test_kucoin_token_masking():
    """Test that KuCoin WebSocket tokens are masked."""
    filter = SensitiveDataFilter()

    # Mock log record
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Connection to wss://ws-api-spot.kucoin.com/?token=2neAiuYvAU61ZDXANAGAsiL4-iAExhsBXZxftpOeh_55i3Ysy2",
        args=(),
        exc_info=None,
    )

    # Apply filter
    filter.filter(record)

    # Verify token is masked
    assert "2neAiuYvAU61ZDXANAGAsiL4" not in record.msg
    assert "?token=***MASKED***" in record.msg
    assert "wss://ws-api-spot.kucoin.com" in record.msg


def test_api_key_masking():
    """Test that API keys are masked."""
    filter = SensitiveDataFilter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="api_key=abcdefghijklmnopqrstuvwxyz1234567890",
        args=(),
        exc_info=None,
    )

    filter.filter(record)

    assert "abcdefghijklmnopqrstuvwxyz" not in record.msg
    assert "api_key=***MASKED***" in record.msg


def test_secret_masking():
    """Test that secrets are masked."""
    filter = SensitiveDataFilter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="secret: mysecretkey123456789012345",
        args=(),
        exc_info=None,
    )

    filter.filter(record)

    assert "mysecretkey123456789012345" not in record.msg
    assert "secret: ***MASKED***" in record.msg


def test_normal_messages_unaffected():
    """Test that normal log messages are not affected."""
    filter = SensitiveDataFilter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="ARB OK | XRP/USDT buy=bybit@2.54 sell=kucoin@2.56",
        args=(),
        exc_info=None,
    )

    original_msg = record.msg
    filter.filter(record)

    # Message should be unchanged
    assert record.msg == original_msg
