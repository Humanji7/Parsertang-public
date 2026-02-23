from __future__ import annotations

from .events import BBOEvent


def _norm_symbol(sym: str) -> str:
    if sym.endswith("USDT"):
        return sym[:-4] + "/USDT"
    return sym


def parse_mexc_bbo(msg: dict, ts_recv: int) -> BBOEvent | None:
    data = msg.get("publicbookticker") or {}
    bid = data.get("bidprice")
    ask = data.get("askprice")
    if bid is None or ask is None:
        return None
    ts_ex = int(msg.get("sendtime") or 0)
    sym = msg.get("symbol")
    if not sym:
        return None
    return BBOEvent("mexc", _norm_symbol(sym), float(bid), float(ask), ts_ex, ts_recv)
