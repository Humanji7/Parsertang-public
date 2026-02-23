from parsertang.v2.health_gate import HealthGate, HealthSnapshot
from parsertang.v2.health_metrics import compute_health_snapshot


def test_health_gate_min_ratio():
    gate = HealthGate(fresh_ratio_min=0.8)
    snap = HealthSnapshot(
        fresh_ratio_by_ex={"bybit": 0.9, "okx": 0.85, "mexc": 0.79},
        fresh_ratio_min=0.79,
        fresh_overlap_count=10,
        total_symbols=20,
    )
    decision = gate.evaluate(snap)
    assert decision.healthy is False


def test_health_gate_passes_when_min_ok():
    gate = HealthGate(fresh_ratio_min=0.8)
    snap = HealthSnapshot(
        fresh_ratio_by_ex={"bybit": 0.9, "okx": 0.85, "mexc": 0.80},
        fresh_ratio_min=0.80,
        fresh_overlap_count=12,
        total_symbols=20,
    )
    decision = gate.evaluate(snap)
    assert decision.healthy is True


def test_health_gate_blocks_when_min_ratio_low():
    gate = HealthGate(0.8)
    snap = compute_health_snapshot({}, now_ms=0, max_age_ms=1000)
    assert gate.evaluate(snap).healthy is False
