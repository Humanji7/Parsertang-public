from parsertang.symbol_refresh import RefreshInputs, should_refresh_symbols


def test_no_refresh_before_min_interval():
    inputs = RefreshInputs(
        now_ts=1000.0,
        last_refresh_ts=950.0,
        min_interval_seconds=600,
        arb_ok_delta=0,
        arb_reject_delta=500,
        stale_exchanges=3,
        stale_exchanges_threshold=1,
        min_arb_ok=1,
        min_arb_reject=200,
    )
    should, reason = should_refresh_symbols(inputs)
    assert should is False
    assert reason == "min_interval"


def test_refresh_on_stale_exchanges():
    inputs = RefreshInputs(
        now_ts=2000.0,
        last_refresh_ts=0.0,
        min_interval_seconds=600,
        arb_ok_delta=10,
        arb_reject_delta=10,
        stale_exchanges=2,
        stale_exchanges_threshold=1,
        min_arb_ok=1,
        min_arb_reject=200,
    )
    should, reason = should_refresh_symbols(inputs)
    assert should is True
    assert reason == "stale_exchanges"


def test_refresh_on_low_ok_high_rejects():
    inputs = RefreshInputs(
        now_ts=2000.0,
        last_refresh_ts=0.0,
        min_interval_seconds=600,
        arb_ok_delta=0,
        arb_reject_delta=300,
        stale_exchanges=0,
        stale_exchanges_threshold=1,
        min_arb_ok=1,
        min_arb_reject=200,
    )
    should, reason = should_refresh_symbols(inputs)
    assert should is True
    assert reason == "low_ok_high_reject"


def test_no_refresh_when_rejects_low():
    inputs = RefreshInputs(
        now_ts=2000.0,
        last_refresh_ts=0.0,
        min_interval_seconds=600,
        arb_ok_delta=0,
        arb_reject_delta=50,
        stale_exchanges=0,
        stale_exchanges_threshold=1,
        min_arb_ok=1,
        min_arb_reject=200,
    )
    should, reason = should_refresh_symbols(inputs)
    assert should is False
    assert reason == "no_trigger"
