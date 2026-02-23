from parsertang.config import EXCHANGE_SYMBOL_LIMITS


def test_exchange_symbol_limits_include_gate_and_mexc_defaults():
    assert EXCHANGE_SYMBOL_LIMITS["gate"] > 0
    assert EXCHANGE_SYMBOL_LIMITS["mexc"] > 0
