from __future__ import annotations

import json
import logging
from pathlib import Path
import time

logger = logging.getLogger(__name__)


def _normalize_symbols(items: list[object]) -> list[str]:
    out: list[str] = []
    for item in items:
        s = str(item).strip()
        if not s:
            continue
        out.append(s.upper())
    return out


def load_allowlist(path: str) -> list[str] | None:
    """Load symbol allowlist from a file.

    Supported formats:
    - JSON list: ["AAA/USDT", "BBB/USDT"]
    - JSON object: {"symbols": ["AAA/USDT", ...]}
    - Plain text / CSV: "AAA/USDT,BBB/USDT"

    Returns normalized uppercase symbols or None when empty/unreadable.
    """
    p = Path(path)
    if not p.exists():
        return None

    try:
        text = p.read_text(encoding="utf-8").strip()
    except Exception as exc:  # noqa: BLE001
        logger.warning("ALLOWLIST | read failed path=%s err=%s", path, exc)
        return None

    if not text:
        return None

    if text.startswith("[") or text.startswith("{"):
        try:
            data = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ALLOWLIST | invalid json path=%s err=%s", path, exc)
            return None
        if isinstance(data, list):
            symbols = _normalize_symbols(data)
            return symbols or None
        if isinstance(data, dict):
            raw = data.get("symbols") or []
            if isinstance(raw, list):
                symbols = _normalize_symbols(raw)
                return symbols or None
            return None
        return None

    # CSV / plain text (support newlines)
    parts: list[str] = []
    for line in text.splitlines():
        parts.extend(chunk.strip() for chunk in line.split(","))
    symbols = [s.upper() for s in parts if s]
    return symbols or None


class AllowlistCache:
    def __init__(self, *, label: str) -> None:
        self._label = label
        self._path: str | None = None
        self._symbols: set[str] | None = None
        self._last_checked_monotonic: float = 0.0
        self._last_mtime: float | None = None

    def get(self, path: str | None, *, refresh_seconds: float) -> set[str] | None:
        if not path:
            self._path = None
            self._symbols = None
            self._last_checked_monotonic = 0.0
            self._last_mtime = None
            return None

        if self._path != path:
            self._path = path
            self._symbols = None
            self._last_checked_monotonic = 0.0
            self._last_mtime = None

        now = time.monotonic()
        refresh_seconds = float(refresh_seconds)
        if refresh_seconds > 0 and (now - self._last_checked_monotonic) < refresh_seconds:
            return self._symbols

        try:
            mtime = Path(path).stat().st_mtime
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s | stat failed path=%s err=%s", self._label, path, exc)
            self._last_checked_monotonic = now
            self._last_mtime = None
            self._symbols = None
            return None

        if self._symbols is not None and self._last_mtime == mtime:
            self._last_checked_monotonic = now
            return self._symbols

        symbols = load_allowlist(path)
        self._last_checked_monotonic = now
        self._last_mtime = mtime
        self._symbols = set(symbols or [])
        return self._symbols or None


truth_allowlist_cache = AllowlistCache(label="TRUTH ALLOWLIST")
