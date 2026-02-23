from datetime import datetime

from parsertang.truth_aggregator import aggregate_alert_truth, parse_alert_truth_line


def test_parse_alert_truth_line_ok():
    line = (
        "2026-01-17 10:00:00 | INFO | x | ALERTTRUTH OK | "
        "ABC/USDT buy=okx sell=mexc ws_buy=1 ws_sell=2 rest_buy=1 rest_sell=2"
    )
    parsed = parse_alert_truth_line(line)
    assert parsed is not None
    assert parsed["status"] == "OK"
    assert parsed["symbol"] == "ABC/USDT"
    assert parsed["buy_ex"] == "okx"
    assert parsed["sell_ex"] == "mexc"


def test_aggregate_alert_truth_counts_fail_reasons_only():
    now = datetime(2026, 1, 17, 12, 0, 0)
    lines = [
        "2026-01-17 11:00:00 | INFO | x | ALERTTRUTH OK | AAA/USDT buy=okx sell=mexc reason=rest_consistent",
        "2026-01-17 11:05:00 | INFO | x | ALERTTRUTH FAIL | AAA/USDT buy=okx sell=mexc reason=rest_buy_price",
    ]
    stats = aggregate_alert_truth(lines, now=now, window_hours=24)
    assert stats["summary"]["ok"] == 1
    assert stats["summary"]["fail"] == 1
    assert stats["pairs"]["AAA/USDT"]["reasons"]["rest_buy_price"] == 1
    assert "rest_consistent" not in stats["pairs"]["AAA/USDT"]["reasons"]

