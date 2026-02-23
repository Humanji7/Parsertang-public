import json
import os
import time

from parsertang.truth_gate import truth_gate_status


def test_truth_gate_allows_when_ratio_high(tmp_path):
    summary = {"ok": 98, "fail": 2, "ratio": 98.0}
    path = tmp_path / "truth_summary.json"
    path.write_text(json.dumps(summary))

    allows, ratio, reason = truth_gate_status(
        str(path),
        ratio_min=98.0,
        max_age_seconds=3600,
        now=time.time(),
        refresh_seconds=0.0,
    )
    assert allows is True
    assert ratio == 98.0
    assert reason == "ok"


def test_truth_gate_blocks_when_ratio_low(tmp_path):
    summary = {"ok": 95, "fail": 5, "ratio": 95.0}
    path = tmp_path / "truth_summary.json"
    path.write_text(json.dumps(summary))

    allows, ratio, reason = truth_gate_status(
        str(path),
        ratio_min=98.0,
        max_age_seconds=3600,
        now=time.time(),
        refresh_seconds=0.0,
    )
    assert allows is False
    assert ratio == 95.0
    assert reason == "low_ratio"


def test_truth_gate_blocks_when_missing(tmp_path):
    path = tmp_path / "missing.json"
    allows, ratio, reason = truth_gate_status(
        str(path),
        ratio_min=98.0,
        max_age_seconds=3600,
        now=time.time(),
        refresh_seconds=0.0,
    )
    assert allows is False
    assert ratio == 0.0
    assert reason == "missing"


def test_truth_gate_blocks_when_stale(tmp_path):
    summary = {"ok": 98, "fail": 2, "ratio": 98.0}
    path = tmp_path / "truth_summary.json"
    old_ts = time.time() - 4000
    path.write_text(json.dumps(summary))
    os.utime(path, (old_ts, old_ts))

    allows, ratio, reason = truth_gate_status(
        str(path),
        ratio_min=98.0,
        max_age_seconds=60,
        now=time.time(),
        refresh_seconds=0.0,
    )
    assert allows is False
    assert ratio == 98.0
    assert reason == "stale"


def test_truth_gate_blocks_when_total_low(tmp_path):
    summary = {"ok": 9, "fail": 0, "ratio": 100.0}
    path = tmp_path / "truth_summary.json"
    path.write_text(json.dumps(summary))

    allows, ratio, reason = truth_gate_status(
        str(path),
        ratio_min=98.0,
        max_age_seconds=3600,
        min_total=10,
        now=time.time(),
        refresh_seconds=0.0,
    )
    assert allows is False
    assert ratio == 100.0
    assert reason == "low_total"
