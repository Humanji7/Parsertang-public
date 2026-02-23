from __future__ import annotations

import re
from datetime import datetime
from typing import Any
import json
from pathlib import Path
import asyncio
import logging
from datetime import timedelta

_TRUTH_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| .*?TRUTH (?P<status>OK|FAIL) \| (?P<body>.+)$"
)
_TRUTH_PROBE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| .*?TRUTH PROBE \| (?P<body>.+)$"
)
_ALERTTRUTH_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| .*?ALERTTRUTH (?P<status>OK|FAIL) \| (?P<body>.+)$"
)

logger = logging.getLogger(__name__)


def read_truth_lines(log_path: Path) -> list[str]:
    lines: list[str] = []
    paths = [log_path]
    prefix = f"{log_path.name}."
    for path in sorted(log_path.parent.glob(f"{log_path.name}.*")):
        tail = path.name[len(prefix) :]
        if not tail.isdigit():
            continue
        paths.append(path)
    for path in paths:
        if path.exists():
            lines.extend(path.read_text().splitlines())
    return lines


def parse_truth_line(line: str) -> dict[str, Any] | None:
    match = _TRUTH_RE.match(line)
    if not match:
        return None
    body = match.group("body")
    parts = body.split(" ", 1)
    if not parts:
        return None
    symbol = parts[0]
    buy_match = re.search(r"\bbuy=([^ ]+)", body)
    sell_match = re.search(r"\bsell=([^ ]+)", body)
    buy_ex = buy_match.group(1) if buy_match else "unknown"
    sell_ex = sell_match.group(1) if sell_match else "unknown"
    reason_match = re.search(r"reason=([^ ]+)", body)
    reason = reason_match.group(1) if reason_match else "unknown"
    ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    return {
        "ts": ts,
        "status": match.group("status"),
        "symbol": symbol,
        "buy_ex": buy_ex,
        "sell_ex": sell_ex,
        "reason": reason,
    }


def parse_truth_probe_line(line: str) -> dict[str, Any] | None:
    match = _TRUTH_PROBE_RE.match(line)
    if not match:
        return None
    body = match.group("body")
    parts = body.split(" ", 1)
    if not parts:
        return None
    symbol = parts[0]
    buy_match = re.search(r"\bbuy=([^ ]+)", body)
    sell_match = re.search(r"\bsell=([^ ]+)", body)
    buy_ex = buy_match.group(1) if buy_match else "unknown"
    sell_ex = sell_match.group(1) if sell_match else "unknown"
    ok_match = re.search(r"\bok=([^ ]+)", body)
    ok_raw = (ok_match.group(1) if ok_match else "").strip().lower()
    ok = ok_raw in {"true", "1", "yes"}
    reason_match = re.search(r"reason=([^ ]+)", body)
    reason = reason_match.group(1) if reason_match else "unknown"
    ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    return {
        "ts": ts,
        "status": "OK" if ok else "FAIL",
        "symbol": symbol,
        "buy_ex": buy_ex,
        "sell_ex": sell_ex,
        "reason": reason,
    }


def parse_alert_truth_line(line: str) -> dict[str, Any] | None:
    match = _ALERTTRUTH_RE.match(line)
    if not match:
        return None
    body = match.group("body")
    parts = body.split(" ", 1)
    if not parts:
        return None
    symbol = parts[0]
    buy_match = re.search(r"\bbuy=([^ ]+)", body)
    sell_match = re.search(r"\bsell=([^ ]+)", body)
    buy_ex = buy_match.group(1) if buy_match else "unknown"
    sell_ex = sell_match.group(1) if sell_match else "unknown"
    reason_match = re.search(r"reason=([^ ]+)", body)
    reason = reason_match.group(1) if reason_match else "unknown"
    ts = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
    return {
        "ts": ts,
        "status": match.group("status"),
        "symbol": symbol,
        "buy_ex": buy_ex,
        "sell_ex": sell_ex,
        "reason": reason,
    }


def aggregate_truth(lines, *, now, window_hours: int = 24):
    cutoff = now - timedelta(hours=window_hours)
    pairs = {}
    ok = 0
    fail = 0
    blocked_fail_reasons = {
        "rest_ask_liq",
        "rest_bid_liq",
        "rest_ask_depth",
        "rest_bid_depth",
    }
    for line in lines:
        parsed = parse_truth_line(line)
        if not parsed:
            parsed = parse_truth_probe_line(line)
        if not parsed:
            continue
        if parsed["ts"] < cutoff:
            continue
        symbol = parsed["symbol"]
        status = parsed["status"]
        reason = parsed["reason"]
        if reason.startswith("fee_calc_"):
            reason = reason[len("fee_calc_") :]
        buy_ex = parsed["buy_ex"]
        sell_ex = parsed["sell_ex"]
        entry = pairs.setdefault(symbol, {"ok": 0, "fail": 0, "reasons": {}})
        exchanges = entry.setdefault("exchanges", set())
        if buy_ex and buy_ex != "unknown":
            exchanges.add(buy_ex)
        if sell_ex and sell_ex != "unknown":
            exchanges.add(sell_ex)
        if status == "OK":
            ok += 1
            entry["ok"] += 1
        else:
            # TRUTH ratio is a safety gate for "do we send false alerts".
            # Liquidity/depth rejects are valid blockers, but they don't represent
            # a WS↔REST inconsistency and shouldn't tank the ratio.
            if reason in blocked_fail_reasons:
                ok += 1
                entry["ok"] += 1
                entry["reasons"][f"{reason}_blocked"] = (
                    entry["reasons"].get(f"{reason}_blocked", 0) + 1
                )
            else:
                fail += 1
                entry["fail"] += 1
                entry["reasons"][reason] = entry["reasons"].get(reason, 0) + 1

    for stats in pairs.values():
        exs = stats.get("exchanges")
        if isinstance(exs, set):
            stats["exchanges"] = sorted(exs)

    total = ok + fail
    ratio = (ok / total * 100.0) if total else 0.0
    return {"summary": {"ok": ok, "fail": fail, "ratio": ratio}, "pairs": pairs}


def aggregate_alert_truth(lines, *, now, window_hours: int = 24):
    cutoff = now - timedelta(hours=window_hours)
    pairs = {}
    ok = 0
    fail = 0
    for line in lines:
        parsed = parse_alert_truth_line(line)
        if not parsed:
            continue
        if parsed["ts"] < cutoff:
            continue
        symbol = parsed["symbol"]
        status = parsed["status"]
        reason = parsed["reason"]
        buy_ex = parsed["buy_ex"]
        sell_ex = parsed["sell_ex"]
        entry = pairs.setdefault(symbol, {"ok": 0, "fail": 0, "reasons": {}})
        exchanges = entry.setdefault("exchanges", set())
        if buy_ex and buy_ex != "unknown":
            exchanges.add(buy_ex)
        if sell_ex and sell_ex != "unknown":
            exchanges.add(sell_ex)
        if status == "OK":
            ok += 1
            entry["ok"] += 1
        else:
            fail += 1
            entry["fail"] += 1
            entry["reasons"][reason] = entry["reasons"].get(reason, 0) + 1

    for stats in pairs.values():
        exs = stats.get("exchanges")
        if isinstance(exs, set):
            stats["exchanges"] = sorted(exs)

    total = ok + fail
    ratio = (ok / total * 100.0) if total else 0.0
    return {"summary": {"ok": ok, "fail": fail, "ratio": ratio}, "pairs": pairs}


def compute_truth_allowlist(
    pairs: dict[str, dict],
    *,
    now: datetime,
    ratio_min: float,
    min_samples: int,
    min_exchanges: int,
    hold_hours: int,
    previous_state: dict[str, Any] | None = None,
) -> tuple[list[str], dict[str, Any]]:
    """Compute allowlist from per-pair TRUTH stats with hysteresis.

    A symbol is included when:
    - total samples (ok+fail) >= min_samples AND ratio >= ratio_min
    OR
    - it was previously included and its hold window has not expired.
    """
    min_samples = max(0, int(min_samples))
    min_exchanges = max(0, int(min_exchanges))
    hold_seconds = max(0, int(hold_hours)) * 3600
    now_ts = now.timestamp()

    prev_symbols: dict[str, Any] = {}
    if isinstance(previous_state, dict):
        prev_symbols = dict(previous_state.get("symbols") or {})

    candidates: set[str] = set()
    for symbol, stats in pairs.items():
        ok = int(stats.get("ok", 0) or 0)
        fail = int(stats.get("fail", 0) or 0)
        total = ok + fail
        if total <= 0:
            continue
        if min_exchanges > 0:
            exs = stats.get("exchanges") or []
            if not isinstance(exs, list) or len(exs) < min_exchanges:
                continue
        ratio = ok / total * 100.0
        if total >= min_samples and ratio >= ratio_min:
            candidates.add(symbol)

    allow: set[str] = set(candidates)
    if hold_seconds > 0:
        for symbol, meta in prev_symbols.items():
            if symbol in allow:
                continue
            try:
                added_at = float((meta or {}).get("added_at", 0.0))
            except Exception:
                added_at = 0.0
            if added_at <= 0:
                continue
            if (now_ts - added_at) < hold_seconds:
                allow.add(symbol)

    allowlist = sorted(allow)
    state_symbols: dict[str, dict[str, float]] = {}
    for symbol in allowlist:
        prev = prev_symbols.get(symbol) or {}
        try:
            added_at = float(prev.get("added_at", 0.0))
        except Exception:
            added_at = 0.0
        if added_at <= 0:
            added_at = now_ts
        state_symbols[symbol] = {"added_at": added_at}

    state = {"version": 1, "generated_at": now_ts, "symbols": state_symbols}
    return allowlist, state


def bucket_pairs(pairs: dict[str, dict[str, int]]) -> dict[str, list[str]]:
    buckets = {"green": [], "yellow": [], "red": []}
    for symbol, counts in pairs.items():
        ok = counts.get("ok", 0)
        fail = counts.get("fail", 0)
        total = ok + fail
        ratio = (ok / total * 100.0) if total else 0.0
        if ratio >= 98.0:
            buckets["green"].append(symbol)
        elif ratio >= 95.0:
            buckets["yellow"].append(symbol)
        else:
            buckets["red"].append(symbol)
    return buckets


def render_outputs(out_dir: Path, *, summary: dict, pairs: dict, buckets: dict) -> dict[str, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "summary": out_dir / "truth_summary.json",
        "pairs": out_dir / "truth_pairs.json",
        "green": out_dir / "truth_green.json",
        "yellow": out_dir / "truth_yellow.json",
        "red": out_dir / "truth_red.json",
    }
    outputs["summary"].write_text(json.dumps(summary, sort_keys=True))
    outputs["pairs"].write_text(json.dumps(pairs, sort_keys=True))
    outputs["green"].write_text(json.dumps(buckets.get("green", []), sort_keys=True))
    outputs["yellow"].write_text(json.dumps(buckets.get("yellow", []), sort_keys=True))
    outputs["red"].write_text(json.dumps(buckets.get("red", []), sort_keys=True))
    return outputs


def format_summary(
    *,
    ok: int,
    fail: int,
    ratio: float,
    top_reasons: list[tuple[str, int]] | None = None,
    alerts_on: bool | None = None,
    label: str | None = None,
) -> str:
    reasons = ""
    if top_reasons:
        reasons = " top_reasons=" + ",".join(f"{name}:{count}" for name, count in top_reasons)
    alerts = ""
    if alerts_on is not None:
        alerts = f" alerts={'on' if alerts_on else 'off'}"
    prefix = f"TRUTH 24H {label} |" if label else "TRUTH 24H |"
    return f"{prefix} ok={ok} fail={fail} ratio={ratio:.2f}%{alerts}{reasons}"


def top_reasons(pairs: dict[str, dict], *, limit: int = 3) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for stats in pairs.values():
        for reason, count in stats.get("reasons", {}).items():
            if reason == "fee_calc_ok":
                continue
            counts[reason] = counts.get(reason, 0) + int(count)
    return sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]


def send_tech_summary(text: str) -> None:
    try:
        from parsertang.alerts import AlertService
    except Exception as exc:
        logger.warning("TRUTH SUMMARY | cannot import AlertService: %s", exc)
        return

    service = AlertService()
    if not service.bot:
        logger.info("TRUTH SUMMARY | %s", text)
        return
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(service.send_tech(text))
        else:
            task = loop.create_task(service.send_tech(text))
            task.add_done_callback(AlertService._handle_task_exception)
    except Exception as exc:
        logger.warning("TRUTH SUMMARY | send_tech failed: %s", exc)
