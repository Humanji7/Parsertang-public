from __future__ import annotations

from dataclasses import dataclass

from parsertang.adaptive_symbol_limits import AdaptiveDecision, AdaptiveSymbolLimiter


@dataclass
class _Snapshot:
    now_ts: float
    multi_ex_symbols: int
    stale_exchanges: int


def test_ramp_increases_after_healthy_window() -> None:
    limiter = AdaptiveSymbolLimiter(
        base_limits={"bybit": 10, "okx": 10, "gate": 5},
        max_limits={"bybit": 14, "okx": 12, "gate": 9},
        core_exchanges=["bybit", "okx"],
        periphery_exchanges=["gate"],
        step_core=2,
        step_periphery=2,
        window_seconds=300,
        min_multi_ex_symbols=55,
        max_stale_exchanges=1,
    )

    assert limiter.current_limits["bybit"] == 10

    decision1 = limiter.evaluate(_Snapshot(0, 60, 0))
    decision2 = limiter.evaluate(_Snapshot(299, 60, 0))
    decision3 = limiter.evaluate(_Snapshot(300, 60, 0))

    assert decision1 is None
    assert decision2 is None
    assert isinstance(decision3, AdaptiveDecision)
    assert decision3.action == "increase"
    assert decision3.limits["bybit"] == 12
    assert decision3.limits["okx"] == 12
    assert decision3.limits["gate"] == 7


def test_ramp_respects_max_limits() -> None:
    limiter = AdaptiveSymbolLimiter(
        base_limits={"bybit": 10},
        max_limits={"bybit": 11},
        core_exchanges=["bybit"],
        periphery_exchanges=[],
        step_core=2,
        step_periphery=0,
        window_seconds=60,
        min_multi_ex_symbols=55,
        max_stale_exchanges=0,
    )

    limiter.evaluate(_Snapshot(0, 60, 0))
    decision = limiter.evaluate(_Snapshot(60, 60, 0))

    assert decision is not None
    assert decision.limits["bybit"] == 11


def test_ramp_decreases_on_unhealthy() -> None:
    limiter = AdaptiveSymbolLimiter(
        base_limits={"bybit": 10},
        max_limits={"bybit": 14},
        core_exchanges=["bybit"],
        periphery_exchanges=[],
        step_core=2,
        step_periphery=0,
        window_seconds=60,
        min_multi_ex_symbols=55,
        max_stale_exchanges=0,
    )

    limiter.evaluate(_Snapshot(0, 60, 0))
    limiter.evaluate(_Snapshot(60, 60, 0))
    assert limiter.current_limits["bybit"] == 12

    decision = limiter.evaluate(_Snapshot(90, 10, 2))

    assert decision is not None
    assert decision.action == "decrease"
    assert decision.limits["bybit"] == 10
