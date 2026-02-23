from parsertang.rotation import build_allowlist_line, select_batch


def test_select_batch_wraps():
    symbols = [f"SYM{i}" for i in range(10)]
    batch = select_batch(symbols, batch_size=3, batch_index=3)
    assert batch == ["SYM9"]


def test_build_allowlist_line():
    line = build_allowlist_line(["AAA/USDT", "BBB/USDT"])
    assert line == 'SYMBOL_ALLOWLIST="AAA/USDT,BBB/USDT"'
