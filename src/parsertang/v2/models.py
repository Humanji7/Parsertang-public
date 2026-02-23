from dataclasses import dataclass
from typing import Any


@dataclass
class Event:
    ex: str
    channel: str
    symbol: str
    ts_ex: float
    ts_recv: float
    data: Any
