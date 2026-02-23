"""Tests for non-retriable WebSocket error classification.

These checks prevent infinite retry loops (and log spam) when an exchange returns
permanent symbol-level errors like "invalid symbol".
"""

from parsertang.streams import _is_non_retriable_ws_error_message


def test_invalid_symbol_is_non_retriable() -> None:
    assert _is_non_retriable_ws_error_message(
        '{"status":"error","err-code":"bad-request","err-msg":"invalid symbol"}'
    )


def test_symbol_not_found_is_non_retriable() -> None:
    assert _is_non_retriable_ws_error_message("symbol not found")


def test_ping_pong_timeout_is_retriable() -> None:
    assert not _is_non_retriable_ws_error_message("ping-pong keepalive missing on time")
