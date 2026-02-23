from dataclasses import dataclass


@dataclass(frozen=True)
class BBOEvent:
    ex: str
    symbol: str
    bid: float
    ask: float
    ts_ex: int
    ts_recv: int
