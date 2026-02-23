import logging

from parsertang.logging_conf import SamplingFilter


def test_sampling_filter_always_allows_markets_symbols_and_ws_health_lines():
    filt = SamplingFilter(ratio=1000)

    allow_messages = [
        "MARKETS | gate markets=123",
        "SYMBOLS | gate allocated 10 symbols (max=30)",
        "WS INIT | ok=gate failed=- unsupported=-",
        "WS INIT FAILED | gate: some error",
        "WS HEALTH | gate=0/0sym",
        "WS LEGACY | gate using per-symbol mode (10 symbols)",
        "WS SKIP | gate no symbols allocated; not starting worker",
    ]

    for message in allow_messages:
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg=message,
            args=(),
            exc_info=None,
        )
        assert filt.filter(record) is True
