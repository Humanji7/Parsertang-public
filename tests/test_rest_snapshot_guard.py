"""Unit tests for REST snapshot restart guard."""

from parsertang.rest_snapshot_guard import (
    RestSnapshotInputs,
    should_restart_rest_snapshot,
)


def test_restart_when_ok_zero() -> None:
    inputs = RestSnapshotInputs(
        now_ts=1000.0,
        ok_count=0,
        err_count=50,
        min_samples=20,
        err_rate_threshold=0.7,
        min_ok=1,
        last_restart_ts=0.0,
        cooldown_seconds=60,
    )
    should_restart, reason = should_restart_rest_snapshot(inputs)
    assert should_restart is True
    assert reason == "ok_zero"


def test_restart_when_err_rate_exceeds_threshold() -> None:
    inputs = RestSnapshotInputs(
        now_ts=1000.0,
        ok_count=10,
        err_count=40,
        min_samples=20,
        err_rate_threshold=0.7,
        min_ok=1,
        last_restart_ts=0.0,
        cooldown_seconds=60,
    )
    should_restart, reason = should_restart_rest_snapshot(inputs)
    assert should_restart is True
    assert reason == "err_rate"


def test_no_restart_when_below_min_samples() -> None:
    inputs = RestSnapshotInputs(
        now_ts=1000.0,
        ok_count=0,
        err_count=5,
        min_samples=20,
        err_rate_threshold=0.7,
        min_ok=1,
        last_restart_ts=0.0,
        cooldown_seconds=60,
    )
    should_restart, reason = should_restart_rest_snapshot(inputs)
    assert should_restart is False
    assert reason == ""


def test_no_restart_during_cooldown() -> None:
    inputs = RestSnapshotInputs(
        now_ts=1000.0,
        ok_count=0,
        err_count=50,
        min_samples=20,
        err_rate_threshold=0.7,
        min_ok=1,
        last_restart_ts=980.0,
        cooldown_seconds=60,
    )
    should_restart, reason = should_restart_rest_snapshot(inputs)
    assert should_restart is False
    assert reason == ""
