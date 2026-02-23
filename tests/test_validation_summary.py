from parsertang.core.metrics_logger import (
    format_truth_probe_summary,
    format_validation_summary,
)


def test_format_validation_summary():
    text = format_validation_summary(valid=95, invalid=5)
    assert "valid=95" in text
    assert "invalid=5" in text
    assert "ratio=95.0%" in text


def test_format_truth_probe_summary():
    text = format_truth_probe_summary(ok=12, fail=3)
    assert "TRUTH SUMMARY" in text
    assert "ok=12" in text
    assert "fail=3" in text
    assert "ratio=80.0%" in text
