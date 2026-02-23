from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class _TruthGateCache:
    path: str | None = None
    read_at: float = 0.0
    ratio: float = 0.0
    allows: bool = False
    reason: str = "missing"


_CACHE = _TruthGateCache()


def truth_gate_status(
    path: str,
    *,
    ratio_min: float,
    max_age_seconds: float,
    min_total: int = 0,
    now: float | None = None,
    refresh_seconds: float = 5.0,
) -> tuple[bool, float, str]:
    now = time.time() if now is None else now
    if (
        refresh_seconds > 0
        and _CACHE.path == path
        and (now - _CACHE.read_at) < refresh_seconds
    ):
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    try:
        stat = os.stat(path)
    except FileNotFoundError:
        _update_cache(path, now, False, 0.0, "missing")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason
    except Exception as exc:
        logger.warning("TRUTH GATE | stat failed: %s", exc)
        _update_cache(path, now, False, 0.0, "error")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    age = max(0.0, now - stat.st_mtime)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        ratio = float(data.get("ratio", 0.0))
        ok = int(data.get("ok", 0) or 0)
        fail = int(data.get("fail", 0) or 0)
        total = ok + fail
    except Exception as exc:
        logger.warning("TRUTH GATE | invalid summary: %s", exc)
        _update_cache(path, now, False, 0.0, "invalid")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    if age > max_age_seconds:
        _update_cache(path, now, False, ratio, "stale")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    if min_total > 0 and total < min_total:
        _update_cache(path, now, False, ratio, "low_total")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    if ratio < ratio_min:
        _update_cache(path, now, False, ratio, "low_ratio")
        return _CACHE.allows, _CACHE.ratio, _CACHE.reason

    _update_cache(path, now, True, ratio, "ok")
    return _CACHE.allows, _CACHE.ratio, _CACHE.reason


def _update_cache(path: str, now: float, allows: bool, ratio: float, reason: str) -> None:
    _CACHE.path = path
    _CACHE.read_at = now
    _CACHE.allows = allows
    _CACHE.ratio = ratio
    _CACHE.reason = reason
