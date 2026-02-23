from datetime import datetime

from parsertang.truth_aggregator import (
    aggregate_truth,
    parse_truth_line,
    parse_truth_probe_line,
)


def test_parse_truth_line_ok():
    line = (
        "2026-01-15 10:00:00 | INFO | x | TRUTH OK | "
        "ABC/USDT buy=okx sell=mexc reason=fee_calc_ok rest_buy=1 rest_sell=2"
    )
    parsed = parse_truth_line(line)
    assert parsed["status"] == "OK"
    assert parsed["symbol"] == "ABC/USDT"
    assert parsed["reason"] == "fee_calc_ok"
    assert parsed["buy_ex"] == "okx"
    assert parsed["sell_ex"] == "mexc"


def test_aggregate_truth_counts():
    now = datetime(2026, 1, 15, 12, 0, 0)
    lines = [
        "2026-01-15 11:00:00 | INFO | x | TRUTH OK | AAA/USDT reason=fee_calc_ok",
        "2026-01-15 11:05:00 | INFO | x | TRUTH FAIL | AAA/USDT reason=ws_stale",
    ]
    stats = aggregate_truth(lines, now=now, window_hours=24)
    assert stats["pairs"]["AAA/USDT"]["ok"] == 1
    assert stats["pairs"]["AAA/USDT"]["fail"] == 1


def test_aggregate_truth_treats_rest_liquidity_fail_as_ok_blocked():
    now = datetime(2026, 1, 15, 12, 0, 0)
    lines = [
        "2026-01-15 11:00:00 | INFO | x | TRUTH OK | AAA/USDT reason=fee_calc_ok",
        "2026-01-15 11:05:00 | INFO | x | TRUTH FAIL | AAA/USDT reason=rest_ask_liq",
    ]
    stats = aggregate_truth(lines, now=now, window_hours=24)
    assert stats["summary"]["ok"] == 2
    assert stats["summary"]["fail"] == 0
    assert stats["pairs"]["AAA/USDT"]["ok"] == 2
    assert stats["pairs"]["AAA/USDT"]["fail"] == 0
    assert stats["pairs"]["AAA/USDT"]["reasons"]["rest_ask_liq_blocked"] == 1


def test_parse_truth_probe_line_and_counts_towards_summary():
    now = datetime(2026, 1, 15, 12, 0, 0)
    line = (
        "2026-01-15 11:00:00 | INFO | x | TRUTH PROBE | "
        "AAA/USDT buy=bybit sell=okx ok=True reason=fee_calc_ok rest_buy=1 rest_sell=2"
    )
    parsed = parse_truth_probe_line(line)
    assert parsed is not None
    assert parsed["status"] == "OK"
    assert parsed["symbol"] == "AAA/USDT"
    assert parsed["buy_ex"] == "bybit"
    assert parsed["sell_ex"] == "okx"

    stats = aggregate_truth([line], now=now, window_hours=24)
    assert stats["summary"]["ok"] == 1
    assert stats["summary"]["fail"] == 0
    assert stats["pairs"]["AAA/USDT"]["ok"] == 1


def test_bucket_pairs():
    from parsertang.truth_aggregator import bucket_pairs

    pairs = {
        "AAA/USDT": {"ok": 98, "fail": 2},
        "BBB/USDT": {"ok": 96, "fail": 4},
        "CCC/USDT": {"ok": 90, "fail": 10},
    }
    buckets = bucket_pairs(pairs)
    assert "AAA/USDT" in buckets["green"]
    assert "BBB/USDT" in buckets["yellow"]
    assert "CCC/USDT" in buckets["red"]


def test_render_outputs_paths(tmp_path):
    from parsertang.truth_aggregator import render_outputs

    render_outputs(tmp_path, summary={"ok": 1, "fail": 0}, pairs={}, buckets={})
    assert (tmp_path / "truth_summary.json").exists()


def test_format_summary():
    from parsertang.truth_aggregator import format_summary

    text = format_summary(
        ok=98,
        fail=2,
        ratio=98.0,
        top_reasons=[("ws_stale", 3)],
        alerts_on=True,
    )
    assert "TRUTH 24H" in text
    assert "alerts=on" in text


def test_format_summary_label():
    from parsertang.truth_aggregator import format_summary

    text = format_summary(ok=1, fail=0, ratio=100.0, label="A")
    assert text.startswith("TRUTH 24H A |")


def test_top_reasons():
    from parsertang.truth_aggregator import top_reasons

    pairs = {
        "AAA/USDT": {"reasons": {"ws_stale": 2, "liquidity": 1}},
        "BBB/USDT": {"reasons": {"ws_stale": 3, "fees": 1}},
    }
    top = top_reasons(pairs, limit=2)
    assert top[0][0] == "ws_stale"
    assert top[0][1] == 5


def test_top_reasons_skips_ok_reason():
    from parsertang.truth_aggregator import top_reasons

    pairs = {
        "AAA/USDT": {"reasons": {"fee_calc_ok": 10, "ws_stale": 2}},
    }
    top = top_reasons(pairs, limit=3)
    assert all(reason != "fee_calc_ok" for reason, _ in top)


def test_read_truth_lines_includes_rotated(tmp_path):
    from parsertang.truth_aggregator import read_truth_lines

    main = tmp_path / "parsertang.log"
    rot1 = tmp_path / "parsertang.log.1"
    rot2 = tmp_path / "parsertang.log.2"
    rot_bad = tmp_path / "parsertang.log.bad"

    main.write_text("2026-01-15 10:00:00 | INFO | x | TRUTH OK | AAA/USDT reason=fee_calc_ok\n")
    rot1.write_text("2026-01-15 10:01:00 | INFO | x | TRUTH FAIL | AAA/USDT reason=ws_stale\n")
    rot2.write_text("2026-01-15 10:02:00 | INFO | x | TRUTH OK | BBB/USDT reason=fee_calc_ok\n")
    rot_bad.write_text("2026-01-15 10:03:00 | INFO | x | TRUTH OK | BAD/USDT reason=fee_calc_ok\n")

    lines = read_truth_lines(main)
    assert any("TRUTH OK" in line for line in lines)
    assert any("TRUTH FAIL" in line for line in lines)
    assert all("BAD/USDT" not in line for line in lines)


def test_compute_truth_allowlist_holds_recent_symbols():
    from datetime import datetime, timedelta

    from parsertang.truth_aggregator import compute_truth_allowlist

    now = datetime(2026, 1, 16, 12, 0, 0)
    pairs = {
        "AAA/USDT": {"ok": 100, "fail": 0, "reasons": {}, "exchanges": ["bybit", "okx", "mexc"]},
        "BBB/USDT": {"ok": 99, "fail": 1, "reasons": {}, "exchanges": ["bybit", "okx", "mexc"]},
        "CCC/USDT": {"ok": 50, "fail": 0, "reasons": {}, "exchanges": ["bybit", "okx"]},
    }
    prev_state = {
        "version": 1,
        "symbols": {
            "BBB/USDT": {"added_at": (now - timedelta(hours=1)).timestamp()},
            "CCC/USDT": {"added_at": (now - timedelta(hours=4)).timestamp()},
        },
    }

    allowlist, state = compute_truth_allowlist(
        pairs,
        now=now,
        ratio_min=98.0,
        min_samples=100,
        min_exchanges=3,
        hold_hours=3,
        previous_state=prev_state,
    )
    assert "AAA/USDT" in allowlist
    assert "BBB/USDT" in allowlist  # held even though ratio < 98
    assert "CCC/USDT" not in allowlist  # dropped (hold expired)
    assert state["symbols"]["BBB/USDT"]["added_at"] == prev_state["symbols"]["BBB/USDT"]["added_at"]
