from __future__ import annotations

from parsertang.fee_debug import (
    RateLimiter,
    is_fee_debug_enabled,
    parse_debug_fee_symbols,
)


def test_parse_debug_fee_symbols_disabled() -> None:
    assert parse_debug_fee_symbols(None) is None
    assert parse_debug_fee_symbols("") is None
    assert parse_debug_fee_symbols("   ") is None


def test_parse_debug_fee_symbols_all() -> None:
    assert parse_debug_fee_symbols("*") == {"*"}


def test_parse_debug_fee_symbols_csv() -> None:
    parsed = parse_debug_fee_symbols("apt/usdt, ARB/USDT")
    assert parsed == {"APT/USDT", "ARB/USDT"}


def test_is_fee_debug_enabled() -> None:
    assert is_fee_debug_enabled("APT/USDT", None) is False
    assert is_fee_debug_enabled("APT/USDT", {"*"}) is True
    assert is_fee_debug_enabled("apt/usdt", {"APT/USDT"}) is True
    assert is_fee_debug_enabled("BTC/USDT", {"APT/USDT"}) is False


def test_rate_limiter_should_log() -> None:
    ts = {"now": 0.0}

    def clock() -> float:
        return ts["now"]

    limiter = RateLimiter(interval_seconds=60.0, clock=clock)
    assert limiter.should_log("k") is True
    assert limiter.should_log("k") is False

    ts["now"] = 59.9
    assert limiter.should_log("k") is False

    ts["now"] = 60.0
    assert limiter.should_log("k") is True
