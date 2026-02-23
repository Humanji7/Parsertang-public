from __future__ import annotations

from .events import BBOEvent


def _norm_symbol(inst_id: str) -> str:
    return inst_id.replace("-", "/")


def parse_okx_bbo(msg: dict, ts_recv: int) -> BBOEvent | None:
    data = msg.get("data") or []
    if not data:
        return None
    item = data[0]
    inst_id = (msg.get("arg") or {}).get("instId") or item.get("instId")
    if not inst_id:
        return None

    # OKX can emit best bid/ask in different public channels:
    # - bbo-tbt: item["bids"][0][0] / item["asks"][0][0]
    # - tickers: item["bidPx"] / item["askPx"]
    bid = None
    ask = None
    bids = item.get("bids") or []
    asks = item.get("asks") or []
    if bids and asks:
        try:
            bid = float(bids[0][0])
            ask = float(asks[0][0])
        except Exception:  # noqa: BLE001
            bid = None
            ask = None
    else:
        try:
            bid_px = item.get("bidPx")
            ask_px = item.get("askPx")
            if bid_px is not None and ask_px is not None:
                bid = float(bid_px)
                ask = float(ask_px)
        except Exception:  # noqa: BLE001
            bid = None
            ask = None

    if bid is None or ask is None:
        return None

    ts_ex = int(item.get("ts") or 0)
    return BBOEvent("okx", _norm_symbol(inst_id), bid, ask, ts_ex, ts_recv)
