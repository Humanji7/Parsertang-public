from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class Level(str, Enum):
    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"


@dataclass
class GuardMetrics:
    stale_exchanges: int
    multi_ex_symbols: int
    tick_lag: float
    queue_depth: int


@dataclass
class GuardConfig:
    stale_warn: int = 1
    stale_critical: int = 3
    tick_warn: float = 60.0
    tick_critical: float = 120.0
    queue_rising_threshold: Optional[int] = None  # placeholder for future use


@dataclass
class GuardDecision:
    level: Level
    reason: str
    actions: List[str]


class Guard:
    def __init__(self, config: Optional[GuardConfig] = None):
        self.config = config or GuardConfig()

    def evaluate(self, m: GuardMetrics) -> GuardDecision:
        # L3
        if m.multi_ex_symbols == 0 or m.tick_lag > self.config.tick_critical:
            return GuardDecision(
                level=Level.L3,
                reason="multi_ex_symbols=0 or tick_lag>critical",
                actions=["restart"],
            )

        # L2
        if m.stale_exchanges >= self.config.stale_critical:
            return GuardDecision(
                level=Level.L2,
                reason="stale_exchanges>=critical",
                actions=["circuit_breaker", "trim_minimal"],
            )

        # L1
        if (
            m.stale_exchanges >= self.config.stale_warn
            or m.tick_lag > self.config.tick_warn
        ):
            return GuardDecision(
                level=Level.L1,
                reason="stale_exchanges>=warn or tick_lag>warn",
                actions=["trim_symbols", "soft_reconnect"],
            )

        # L0
        return GuardDecision(level=Level.L0, reason="healthy", actions=[])
