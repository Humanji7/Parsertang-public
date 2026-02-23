from parsertang.v2.guard import Guard, GuardMetrics, Level


def test_guard_goes_l1_on_single_stale():
    g = Guard()
    d = g.evaluate(
        GuardMetrics(stale_exchanges=1, multi_ex_symbols=10, tick_lag=10, queue_depth=0)
    )
    assert d.level == Level.L1


def test_guard_goes_l2_on_three_stale():
    g = Guard()
    d = g.evaluate(
        GuardMetrics(stale_exchanges=3, multi_ex_symbols=10, tick_lag=10, queue_depth=0)
    )
    assert d.level == Level.L2


def test_guard_goes_l3_on_zero_overlap():
    g = Guard()
    d = g.evaluate(
        GuardMetrics(stale_exchanges=0, multi_ex_symbols=0, tick_lag=10, queue_depth=0)
    )
    assert d.level == Level.L3


def test_guard_respects_tick_critical():
    g = Guard()
    d = g.evaluate(
        GuardMetrics(stale_exchanges=0, multi_ex_symbols=5, tick_lag=200, queue_depth=0)
    )
    assert d.level == Level.L3
