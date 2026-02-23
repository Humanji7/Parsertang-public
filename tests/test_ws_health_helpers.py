from collections import defaultdict

from parsertang.core.metrics_logger import (
    _format_ws_exchange_stat,
    _update_ws_stale_intervals,
)


def test_format_ws_exchange_stat_includes_alloc_and_stale() -> None:
    s = _format_ws_exchange_stat(
        ex_id="gate",
        updates=0,
        unique_symbols=0,
        allocated=30,
        stale_intervals=5,
    )
    assert "gate=0/0sym" in s
    assert "alloc=30" in s
    assert "stale=5" in s


def test_update_ws_stale_intervals_increments_and_resets() -> None:
    stale = defaultdict(int)

    _update_ws_stale_intervals(stale, ex_id="gate", allocated=30, updates=0)
    assert stale["gate"] == 1

    _update_ws_stale_intervals(stale, ex_id="gate", allocated=30, updates=0)
    assert stale["gate"] == 2

    _update_ws_stale_intervals(stale, ex_id="gate", allocated=30, updates=10)
    assert stale["gate"] == 0

    # If nothing is allocated, staleness should not accumulate.
    _update_ws_stale_intervals(stale, ex_id="gate", allocated=0, updates=0)
    assert stale["gate"] == 0
