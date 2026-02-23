"""Restart guard for REST snapshot loop."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RestSnapshotInputs:
    now_ts: float
    ok_count: int
    err_count: int
    min_samples: int
    err_rate_threshold: float
    min_ok: int
    last_restart_ts: float
    cooldown_seconds: int


def should_restart_rest_snapshot(
    inputs: RestSnapshotInputs,
) -> tuple[bool, str]:
    total = inputs.ok_count + inputs.err_count
    if total < inputs.min_samples:
        return False, ""
    if inputs.cooldown_seconds > 0:
        if (inputs.now_ts - inputs.last_restart_ts) < inputs.cooldown_seconds:
            return False, ""
    if inputs.ok_count < inputs.min_ok:
        return True, "ok_zero"
    if total > 0:
        err_rate = inputs.err_count / total
        if err_rate >= inputs.err_rate_threshold:
            return True, "err_rate"
    return False, ""
