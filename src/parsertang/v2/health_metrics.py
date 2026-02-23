from collections import defaultdict
from collections.abc import Iterable

from parsertang.v2.health_gate import HealthSnapshot
from parsertang.v2.models import Event


def compute_health_snapshot(
    events: dict[tuple[str, str], Event],
    now_ms: float,
    max_age_ms: float,
    expected_exchanges: Iterable[str] | None = None,
) -> HealthSnapshot:
    """Compute health snapshot metrics; all times are in milliseconds."""
    total_by_ex: dict[str, int] = defaultdict(int)
    fresh_by_ex: dict[str, int] = defaultdict(int)

    if expected_exchanges is not None:
        for ex in expected_exchanges:
            total_by_ex[ex] += 0

    for (ex, _sym), ev in events.items():
        total_by_ex[ex] += 1
        if now_ms - ev.ts_recv <= max_age_ms:
            fresh_by_ex[ex] += 1

    fresh_ratio_by_ex: dict[str, float] = {}
    for ex, total in total_by_ex.items():
        fresh_ratio_by_ex[ex] = (fresh_by_ex[ex] / total) if total else 0.0

    fresh_ratio_min = min(fresh_ratio_by_ex.values()) if fresh_ratio_by_ex else 0.0
    fresh_overlap_count = 0
    total_symbols = len({sym for (_ex, sym) in events.keys()})

    return HealthSnapshot(
        fresh_ratio_by_ex=fresh_ratio_by_ex,
        fresh_ratio_min=fresh_ratio_min,
        fresh_overlap_count=fresh_overlap_count,
        total_symbols=total_symbols,
    )
