from dataclasses import dataclass


@dataclass
class HealthSnapshot:
    fresh_ratio_by_ex: dict[str, float]
    fresh_ratio_min: float
    fresh_overlap_count: int
    total_symbols: int


@dataclass
class HealthDecision:
    healthy: bool


class HealthGate:
    def __init__(self, fresh_ratio_min: float = 0.80) -> None:
        self.fresh_ratio_min = float(fresh_ratio_min)

    def evaluate(self, snapshot: HealthSnapshot) -> HealthDecision:
        return HealthDecision(healthy=snapshot.fresh_ratio_min >= self.fresh_ratio_min)
