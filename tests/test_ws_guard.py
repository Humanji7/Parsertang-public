from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import asyncio
import logging
import pytest


@dataclass
class _WSMetrics:
    update_counters: dict[str, int] = field(default_factory=dict)
    symbols_seen: dict[str, set[str]] = field(default_factory=dict)
    allocated_symbols: dict[str, int] = field(default_factory=dict)
    stale_intervals: dict[str, int] = field(default_factory=dict)
    last_logged: float = 0.0


class _State:
    def __init__(self) -> None:
        self.orderbooks: dict[tuple[str, str], object] = {}
        self.orderbooks_lock = asyncio.Lock()
        self.ws_metrics = _WSMetrics()
        self.metrics_lock = asyncio.Lock()


class _Alert:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_tech(self, text: str) -> None:
        self.messages.append(text)


def _fake_snapshot() -> str:
    return "INCIDENT SNAPSHOT | load=0.01 uptime=1:00 mem=1/2GB"


@pytest.mark.asyncio
async def test_ws_guard_no_restart_when_overlap(tmp_path: Path):
    from parsertang.ws_guard import WSGuard

    state = _State()
    now = datetime.now().timestamp()

    # Two exchanges with the same symbol -> overlap exists
    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "BTC/USDT")] = object()

    guard = WSGuard(
        no_overlap_minutes=2,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )

    decision = await guard.tick(state, now_ts=now)

    assert decision.should_restart is False
    assert decision.no_overlap_minutes == 0
    assert decision.status == "ok"


@pytest.mark.asyncio
async def test_ws_guard_restart_after_no_overlap_threshold(tmp_path: Path):
    from parsertang.ws_guard import WSGuard

    state = _State()
    now = datetime.now().timestamp()

    # No overlap: different symbols per exchange
    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    guard = WSGuard(
        no_overlap_minutes=2,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )

    decision1 = await guard.tick(state, now_ts=now)
    decision2 = await guard.tick(state, now_ts=now + 60)

    assert decision1.should_restart is False
    assert decision1.no_overlap_minutes == 1

    assert decision2.should_restart is True
    assert decision2.no_overlap_minutes == 2
    assert decision2.status == "degraded"


@pytest.mark.asyncio
async def test_guard_once_triggers_restart(tmp_path: Path):
    from parsertang.ws_guard import WSGuard, guard_once

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )
    alert = _Alert()
    restarted = {"value": False}

    def exit_fn() -> None:
        restarted["value"] = True

    decision = await guard_once(state, alert, guard, now_ts=now, exit_fn=exit_fn)

    assert decision.should_restart is True
    assert restarted["value"] is True
    assert alert.messages


@pytest.mark.asyncio
async def test_ws_guard_report_includes_threshold(tmp_path: Path):
    from parsertang.ws_guard import WSGuard

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )

    decision = await guard.tick(state, now_ts=now)

    assert decision.should_restart is True
    assert decision.report
    assert "no_overlap=1m" in decision.report


@pytest.mark.asyncio
async def test_guard_once_l1_recover_skips_restart(tmp_path: Path):
    from parsertang.ws_guard import WSGuard, guard_once

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )
    alert = _Alert()
    restarted = {"value": False}

    def exit_fn() -> None:
        restarted["value"] = True

    async def recover_fn(_stale_exchanges=None) -> bool:
        return True

    decision = await guard_once(
        state,
        alert,
        guard,
        now_ts=now,
        exit_fn=exit_fn,
        recover_fn=recover_fn,
    )

    assert decision.should_restart is True
    assert restarted["value"] is False
    assert alert.messages
    assert "action=L1" in alert.messages[-1]


@pytest.mark.asyncio
async def test_guard_once_no_overlap_prefers_l0_for_stale_exchanges(
    tmp_path: Path,
):
    from parsertang.ws_guard import WSGuard, guard_once

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    async with state.metrics_lock:
        state.ws_metrics.allocated_symbols = {"bybit": 1, "okx": 1}
        state.ws_metrics.stale_intervals = {"bybit": 2}

    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
        stale_exchanges_threshold=1,
    )
    alert = _Alert()
    restarted = {"value": False}
    seen: dict[str, list[str] | None] = {"stale": None}

    def exit_fn() -> None:
        restarted["value"] = True

    async def recover_fn(stale_exchanges=None) -> bool:
        seen["stale"] = stale_exchanges
        return True

    decision = await guard_once(
        state,
        alert,
        guard,
        now_ts=now,
        exit_fn=exit_fn,
        recover_fn=recover_fn,
    )

    assert decision.should_restart is True
    assert restarted["value"] is False
    assert seen["stale"] == ["bybit"]
    assert alert.messages
    assert "action=L0" in alert.messages[-1]


@pytest.mark.asyncio
async def test_ws_guard_triggers_on_stale_exchanges(tmp_path: Path):
    from parsertang.ws_guard import WSGuard

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "BTC/USDT")] = object()

    async with state.metrics_lock:
        state.ws_metrics.allocated_symbols = {"bybit": 1, "okx": 1}
        state.ws_metrics.stale_intervals = {"bybit": 3}

    guard = WSGuard(
        no_overlap_minutes=10,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
        stale_exchanges_threshold=1,
    )

    decision = await guard.tick(state, now_ts=now)

    assert decision.should_restart is True
    assert decision.report
    assert "trigger=stale_exchanges" in decision.report


def _make_state_with_alloc_zero() -> _State:
    state = _State()
    state.ws_metrics.allocated_symbols = {"bybit": 0, "okx": 0}
    return state


@pytest.mark.asyncio
async def test_ws_guard_triggers_on_zero_allocation(tmp_path: Path):
    from parsertang.ws_guard import WSGuard

    state = _make_state_with_alloc_zero()
    now = datetime.now().timestamp()

    guard = WSGuard(
        no_overlap_minutes=60,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
    )

    decision = await guard.tick(state, now_ts=now)

    assert decision.should_restart is True
    assert decision.report
    assert "trigger=alloc_zero" in decision.report


@pytest.mark.asyncio
async def test_guard_once_writes_report_to_log(tmp_path: Path):
    from parsertang.ws_guard import WSGuard, guard_once

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    log_path = tmp_path / "parsertang.log"
    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=log_path,
    )
    alert = _Alert()

    await guard_once(state, alert, guard, now_ts=now)

    assert log_path.exists()
    contents = log_path.read_text()
    assert "WS ALERT |" in contents
    assert "action=" in contents


@pytest.mark.asyncio
async def test_ws_guard_logs_tick_lag(tmp_path: Path, caplog):
    from parsertang.ws_guard import WSGuard

    state = _State()
    guard = WSGuard(
        no_overlap_minutes=999,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=tmp_path / "parsertang.log",
        check_interval_seconds=60,
    )

    with caplog.at_level(logging.WARNING):
        await guard.tick(state, now_ts=0)
        await guard.tick(state, now_ts=200)

    assert "WS GUARD | tick lag" in caplog.text


@pytest.mark.asyncio
async def test_guard_once_includes_snapshot_lines(tmp_path: Path):
    from parsertang.ws_guard import WSGuard, guard_once

    state = _State()
    now = datetime.now().timestamp()

    async with state.orderbooks_lock:
        state.orderbooks[("bybit", "BTC/USDT")] = object()
        state.orderbooks[("okx", "ETH/USDT")] = object()

    log_path = tmp_path / "parsertang.log"
    guard = WSGuard(
        no_overlap_minutes=1,
        restart_min_interval_minutes=0,
        state_path=tmp_path / "ws_guard_state.json",
        log_path=log_path,
    )
    alert = _Alert()

    await guard_once(
        state,
        alert,
        guard,
        now_ts=now,
        snapshot_fn=_fake_snapshot,
    )

    assert alert.messages
    assert "INCIDENT SNAPSHOT" in alert.messages[-1]
