from parsertang.refresh_trace import RefreshTrace


def test_refresh_trace_summary_with_first_update():
    trace = RefreshTrace(reason="stale_exchanges", start_ts=100.0)
    trace.mark("close", ts=101.0)
    trace.mark("init", ts=102.0)
    trace.mark("subscribe", ts=103.0)
    trace.mark_first_update("bybit", "BTC/USDT", ts=112.3)

    summary = trace.summary()

    assert "reason=stale_exchanges" in summary
    assert "close=ok" in summary
    assert "init=ok" in summary
    assert "subscribe=ok" in summary
    assert "first_update=bybit:BTC/USDT@12.3s" in summary


def test_refresh_trace_summary_without_first_update():
    trace = RefreshTrace(reason="low_overlap", start_ts=200.0)
    trace.mark("close", ts=201.0)
    trace.mark("init", ts=202.0)
    trace.mark("subscribe", ts=203.0)

    summary = trace.summary()

    assert "reason=low_overlap" in summary
    assert "first_update=none" in summary
