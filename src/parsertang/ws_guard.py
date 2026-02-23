from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class WSGuardDecision:
    should_restart: bool
    status: str
    no_overlap_minutes: int
    trigger: str | None = None
    stale_exchanges: list[str] = field(default_factory=list)
    report: str | None = None


@dataclass
class WSGuardSnapshot:
    multi_ex_symbols: int
    total_symbols: int
    active_exchanges: int
    stale_exchanges: list[str]
    stale_exchanges_count: int
    alloc_zero: bool


class WSGuard:
    def __init__(
        self,
        *,
        no_overlap_minutes: int,
        restart_min_interval_minutes: int,
        state_path: Path,
        log_path: Path,
        min_active_exchanges: int = 3,
        stale_exchanges_threshold: int = 2,
        check_interval_seconds: int = 60,
    ) -> None:
        self.no_overlap_threshold = max(1, int(no_overlap_minutes))
        self.restart_min_interval_seconds = max(
            0, int(restart_min_interval_minutes) * 60
        )
        self.state_path = Path(state_path)
        self.log_path = Path(log_path)
        self.min_active_exchanges = max(1, int(min_active_exchanges))
        self.stale_exchanges_threshold = max(1, int(stale_exchanges_threshold))
        self.check_interval_seconds = max(1, int(check_interval_seconds))

        self.no_overlap_minutes = 0
        self.last_overlap_ts: float | None = None
        self.last_restart_ts: float | None = None
        self._last_tick_ts: float | None = None
        self._last_tick_wall_ts: float | None = None

        self._load_state()

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text())
            self.last_restart_ts = data.get("last_restart_ts")
            self.last_overlap_ts = data.get("last_overlap_ts")
        except Exception:
            return

    def _save_state(self) -> None:
        data = {
            "last_restart_ts": self.last_restart_ts,
            "last_overlap_ts": self.last_overlap_ts,
        }
        try:
            self.state_path.write_text(json.dumps(data))
        except Exception:
            pass

    async def tick(self, state: Any, now_ts: float | None = None) -> WSGuardDecision:
        now_ts = time.time() if now_ts is None else now_ts
        if self._last_tick_wall_ts is not None:
            lag = now_ts - self._last_tick_wall_ts
            if lag > (self.check_interval_seconds * 1.5):
                logger.warning(
                    "WS GUARD | tick lag=%.1fs expected=%ds",
                    lag,
                    self.check_interval_seconds,
                )
        self._last_tick_wall_ts = now_ts

        snapshot = await self._snapshot(state)

        has_overlap = snapshot.multi_ex_symbols > 0
        if has_overlap:
            self.no_overlap_minutes = 0
            self.last_overlap_ts = now_ts
            self._last_tick_ts = now_ts
            self._save_state()

        # Only increment once per interval to avoid double-counting
        if self._last_tick_ts is None or now_ts - self._last_tick_ts >= (
            self.check_interval_seconds * 0.5
        ):
            self.no_overlap_minutes += 1
            self._last_tick_ts = now_ts

        should_restart = (
            (not has_overlap)
            and self._restart_allowed(now_ts)
            and (self.no_overlap_minutes >= self.no_overlap_threshold)
        )

        report = None
        trigger = None
        if should_restart:
            trigger = "no_overlap"
            self.last_restart_ts = now_ts
            self._save_state()
            report = self._build_report(snapshot, now_ts, trigger)
        elif snapshot.alloc_zero and self._restart_allowed(now_ts):
            trigger = "alloc_zero"
            self.last_restart_ts = now_ts
            self._save_state()
            report = self._build_report(snapshot, now_ts, trigger)
            should_restart = True
        elif (
            snapshot.stale_exchanges_count >= self.stale_exchanges_threshold
            and self._restart_allowed(now_ts)
        ):
            trigger = "stale_exchanges"
            self.last_restart_ts = now_ts
            self._save_state()
            report = self._build_report(snapshot, now_ts, trigger)
            should_restart = True

        return WSGuardDecision(
            should_restart=should_restart,
            status="degraded" if should_restart else "ok",
            no_overlap_minutes=self.no_overlap_minutes,
            trigger=trigger,
            stale_exchanges=snapshot.stale_exchanges,
            report=report,
        )

    def _restart_allowed(self, now_ts: float) -> bool:
        if self.restart_min_interval_seconds <= 0:
            return True
        if not self.last_restart_ts:
            return True
        return (now_ts - self.last_restart_ts) >= self.restart_min_interval_seconds

    async def _snapshot(self, state: Any) -> WSGuardSnapshot:
        # Compute overlap from orderbooks (more stable than ws_metrics which resets)
        symbol_exchange_count: dict[str, set[str]] = {}
        async with state.orderbooks_lock:
            for ex_id, symbol in state.orderbooks.keys():
                symbol_exchange_count.setdefault(symbol, set()).add(ex_id)

        multi_ex_symbols = sum(
            1 for exs in symbol_exchange_count.values() if len(exs) >= 2
        )
        total_symbols = len(symbol_exchange_count)
        active_exchanges = len(
            {ex_id for exs in symbol_exchange_count.values() for ex_id in exs}
        )
        alloc_zero = False
        if hasattr(state, "ws_metrics") and hasattr(state, "metrics_lock"):
            async with state.metrics_lock:
                allocated = getattr(state.ws_metrics, "allocated_symbols", {})
                alloc_zero = bool(allocated) and all(v <= 0 for v in allocated.values())

        stale_exchanges: list[str] = []
        if hasattr(state, "ws_metrics") and hasattr(state, "metrics_lock"):
            async with state.metrics_lock:
                stale_intervals = getattr(state.ws_metrics, "stale_intervals", {})
                allocated = getattr(state.ws_metrics, "allocated_symbols", {})
                for ex_id, stale in stale_intervals.items():
                    if allocated.get(ex_id, 0) <= 0:
                        continue
                    if stale >= self.stale_exchanges_threshold:
                        stale_exchanges.append(ex_id)

        return WSGuardSnapshot(
            multi_ex_symbols=multi_ex_symbols,
            total_symbols=total_symbols,
            active_exchanges=active_exchanges,
            stale_exchanges=sorted(stale_exchanges),
            stale_exchanges_count=len(stale_exchanges),
            alloc_zero=alloc_zero,
        )

    def _build_report(
        self, snapshot: WSGuardSnapshot, now_ts: float, trigger: str
    ) -> str:
        now = datetime.fromtimestamp(now_ts)
        last_overlap = (
            datetime.fromtimestamp(self.last_overlap_ts).strftime("%Y-%m-%d %H:%M:%S")
            if self.last_overlap_ts
            else "unknown"
        )
        timeouts_60m, errors_60m, ws_starts_60m = self._scan_logs(now)

        lines = [
            (
                "WS ALERT | "
                f"no_overlap={self.no_overlap_threshold}m "
                f"count={self.no_overlap_minutes}m -> RESTARTING"
            ),
            f"trigger={trigger}",
            f"time={now.strftime('%Y-%m-%d %H:%M:%S')} | last_overlap={last_overlap}",
            (
                "WS SNAPSHOT | multi_ex_symbols="
                f"{snapshot.multi_ex_symbols} total_symbols={snapshot.total_symbols} "
                f"active_exchanges={snapshot.active_exchanges}"
            ),
            (
                "stale_exchanges="
                f"{snapshot.stale_exchanges_count} "
                f"({', '.join(snapshot.stale_exchanges) or '-'})"
            ),
            "timeouts_60m=" + _format_counts(timeouts_60m),
            "errors_60m=" + str(errors_60m),
            "ws_workers_started_60m=" + str(ws_starts_60m),
        ]
        return "\n".join(lines)

    def _scan_logs(self, now: datetime) -> tuple[dict[str, int], int, int]:
        if not self.log_path.exists():
            return {}, 0, 0

        cutoff = now - timedelta(hours=1)
        timeouts: dict[str, int] = {}
        errors = 0
        ws_starts = 0

        try:
            with self.log_path.open() as f:
                for line in f:
                    if len(line) < 19:
                        continue
                    ts = line[:19]
                    try:
                        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue
                    if dt < cutoff:
                        continue

                    if "WS TIMEOUT" in line:
                        # "WS TIMEOUT | <ex> <symbol> ..."
                        try:
                            rest = line.split("WS TIMEOUT |", 1)[1].strip()
                            ex_id = rest.split(" ", 1)[0]
                        except Exception:
                            ex_id = "unknown"
                        timeouts[ex_id] = timeouts.get(ex_id, 0) + 1

                    if "ERROR" in line or "FAILED" in line:
                        errors += 1

                    if "WS WORKER START" in line:
                        ws_starts += 1
        except Exception:
            return {}, 0, 0

        return timeouts, errors, ws_starts

    def append_report(self, report: str, now_ts: float | None = None) -> None:
        timestamp = datetime.fromtimestamp(
            time.time() if now_ts is None else now_ts
        ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with self.log_path.open("a") as f:
                for line in report.splitlines():
                    f.write(f"{timestamp} | {line}\n")
        except Exception:
            pass


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "0"
    parts = [f"{k}={v}" for k, v in sorted(counts.items())]
    return ",".join(parts)


def _format_uptime(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h{minutes:02d}m"


def _read_proc_kv(path: Path) -> dict[str, int]:
    data: dict[str, int] = {}
    try:
        for line in path.read_text().splitlines():
            if ":" not in line:
                continue
            key, rest = line.split(":", 1)
            value = rest.strip().split(" ", 1)[0]
            if value.isdigit():
                data[key] = int(value)
    except Exception:
        return {}
    return data


def build_incident_snapshot() -> str | None:
    """Return a short snapshot string for incident reports."""
    lines: list[str] = []
    parts: list[str] = []

    try:
        load1, load5, load15 = os.getloadavg()
        parts.append(f"load={load1:.2f},{load5:.2f},{load15:.2f}")
    except Exception:
        pass

    uptime_path = Path("/proc/uptime")
    if uptime_path.exists():
        try:
            uptime_seconds = int(float(uptime_path.read_text().split()[0]))
            parts.append(f"uptime={_format_uptime(uptime_seconds)}")
        except Exception:
            pass

    meminfo_path = Path("/proc/meminfo")
    if meminfo_path.exists():
        meminfo = _read_proc_kv(meminfo_path)
        total_kb = meminfo.get("MemTotal")
        avail_kb = meminfo.get("MemAvailable")
        if total_kb and avail_kb is not None:
            used_kb = max(0, total_kb - avail_kb)
            parts.append(
                f"mem={used_kb // 1024}/{total_kb // 1024}MB"
                f" avail={avail_kb // 1024}MB"
            )
        swap_total = meminfo.get("SwapTotal")
        swap_free = meminfo.get("SwapFree")
        if swap_total and swap_free is not None:
            swap_used = max(0, swap_total - swap_free)
            parts.append(f"swap={swap_used // 1024}/{swap_total // 1024}MB")

    if parts:
        lines.append("INCIDENT SNAPSHOT | " + " ".join(parts))

    sockstat_path = Path("/proc/net/sockstat")
    if sockstat_path.exists():
        try:
            sockstat = sockstat_path.read_text().splitlines()
        except Exception:
            sockstat = []
        tcp_stats: dict[str, str] = {}
        for line in sockstat:
            if not line.startswith("TCP:"):
                continue
            tokens = line.replace("TCP:", "").strip().split()
            for i in range(0, len(tokens) - 1, 2):
                tcp_stats[tokens[i]] = tokens[i + 1]
        if tcp_stats:
            fields = []
            for key in ("inuse", "orphan", "tw", "alloc", "mem"):
                if key in tcp_stats:
                    fields.append(f"{key}={tcp_stats[key]}")
            if fields:
                lines.append("INCIDENT SNAPSHOT | tcp " + " ".join(fields))

    return "\n".join(lines) if lines else None


async def guard_once(
    state: Any,
    alert_service: Any,
    guard: WSGuard,
    *,
    now_ts: float | None = None,
    exit_fn: Any | None = None,
    recover_fn: Any | None = None,
    snapshot_fn: Any | None = None,
) -> WSGuardDecision:
    decision = await guard.tick(state, now_ts=now_ts)
    if decision.should_restart and decision.report:
        report = decision.report
        if snapshot_fn is not None:
            try:
                snapshot = snapshot_fn()
            except Exception:
                snapshot = None
            if snapshot:
                report = report + "\n" + str(snapshot)
        did_recover = False
        l0_exchanges: list[str] | None = None
        if decision.trigger in {"stale_exchanges", "no_overlap"}:
            if decision.stale_exchanges:
                l0_exchanges = decision.stale_exchanges
        if recover_fn is not None:
            try:
                if l0_exchanges:
                    did_recover = await recover_fn(l0_exchanges)
                else:
                    did_recover = await recover_fn(None)
            except Exception:
                did_recover = False
        if did_recover:
            if l0_exchanges:
                report = report + "\n" + "action=L0 recover requested"
            else:
                report = report + "\n" + "action=L1 recover requested"
        else:
            report = report + "\n" + "action=L2 restart via systemd"
        guard.append_report(report, now_ts=now_ts)
        if alert_service is not None:
            await alert_service.send_tech(report)
        if not did_recover and exit_fn is not None:
            exit_fn()
    return decision
