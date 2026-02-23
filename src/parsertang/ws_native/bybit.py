from __future__ import annotations

import logging

from .events import BBOEvent


_LOGGER = logging.getLogger(__name__)
_CONTROL_SEEN: set[tuple[str | None, str | None, str | None]] = set()
_DATA_SEEN = False


def _norm_symbol(sym: str) -> str:
    if sym.endswith("USDT"):
        return sym[:-4] + "/USDT"
    return sym


def _extract_price(raw) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (list, tuple)) and raw:
        raw = raw[0]
        if isinstance(raw, (list, tuple)) and raw:
            raw = raw[0]
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_bybit_bbo(msg: dict, ts_recv: int) -> BBOEvent | None:
    if "op" in msg:
        key = (
            msg.get("op"),
            str(msg.get("success")),
            str(msg.get("ret_msg")),
        )
        if key not in _CONTROL_SEEN:
            _CONTROL_SEEN.add(key)
            _LOGGER.info(
                "WSNATIVE BYBIT | control op=%s success=%s msg=%s",
                msg.get("op"),
                msg.get("success"),
                msg.get("ret_msg"),
            )
        return None
    global _DATA_SEEN  # noqa: PLW0603
    data = msg.get("data") or {}
    if not _DATA_SEEN:
        _DATA_SEEN = True
        data_keys = sorted(data.keys()) if isinstance(data, dict) else None
        _LOGGER.info(
            "WSNATIVE BYBIT | data_sample keys=%s topic=%s type=%s data_type=%s data_keys=%s bid=%s ask=%s",
            sorted(msg.keys()),
            msg.get("topic"),
            msg.get("type"),
            type(data).__name__,
            data_keys,
            data.get("bid1Price") if isinstance(data, dict) else None,
            data.get("ask1Price") if isinstance(data, dict) else None,
        )
    if isinstance(data, list):
        items = data
    else:
        items = [data]

    for item in items:
        if not isinstance(item, dict):
            continue
        sym = item.get("symbol") or item.get("s")
        if not sym:
            continue
        bid = _extract_price(item.get("bid1Price") or item.get("b"))
        ask = _extract_price(item.get("ask1Price") or item.get("a"))
        if bid is None or ask is None:
            continue
        ts_ex = int(msg.get("ts") or 0)
        return BBOEvent("bybit", _norm_symbol(sym), bid, ask, ts_ex, ts_recv)
    return None
