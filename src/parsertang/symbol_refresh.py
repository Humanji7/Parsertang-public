from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RefreshInputs:
    now_ts: float
    last_refresh_ts: float
    min_interval_seconds: int
    arb_ok_delta: int
    arb_reject_delta: int
    stale_exchanges: int
    stale_exchanges_threshold: int
    min_arb_ok: int
    min_arb_reject: int


def should_refresh_symbols(inputs: RefreshInputs) -> tuple[bool, str]:
    if inputs.now_ts - inputs.last_refresh_ts < inputs.min_interval_seconds:
        return False, "min_interval"

    if inputs.stale_exchanges >= inputs.stale_exchanges_threshold:
        return True, "stale_exchanges"

    if (
        inputs.arb_ok_delta < inputs.min_arb_ok
        and inputs.arb_reject_delta >= inputs.min_arb_reject
    ):
        return True, "low_ok_high_reject"

    return False, "no_trigger"
