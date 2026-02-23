from parsertang.logging_conf import SamplingFilter


def test_ws_native_logging_allowed():
    joined = " ".join(SamplingFilter.ALWAYS_ALLOW_STARTS)
    assert "WSNATIVE HEALTH" in joined
    assert "WSNATIVE STALE" in joined
