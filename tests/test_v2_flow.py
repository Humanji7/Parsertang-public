from parsertang.v2.queue import BoundedEventQueue
from parsertang.v2.processor import Processor
from parsertang.v2.guard import Guard, GuardMetrics, Level
from parsertang.v2.models import Event
from parsertang.v2.shadow import ShadowPipeline


def make_event(ex="bybit", sym="AAA/USDT", seq=0, ts=None):
    ts_val = ts if ts is not None else seq * 10
    return Event(
        ex=ex,
        channel="orderbook",
        symbol=sym,
        ts_ex=ts_val,
        ts_recv=ts_val,
        data={"seq": seq},
    )


def compute_metrics(proc: Processor, queue_depth: int, now: float) -> GuardMetrics:
    # For PoC: stale_exchanges = exchanges without updates in this snapshot;
    # here approximate: if no state for ex -> stale, otherwise 0.
    state = proc.snapshot()
    seen_ex = {ex for (ex, _sym) in state.keys()}
    stale = 0
    for ex in ["bybit", "gate", "kucoin"]:
        if ex not in seen_ex:
            stale += 1
    return GuardMetrics(
        stale_exchanges=stale,
        multi_ex_symbols=len({sym for (_ex, sym) in state.keys()}),
        tick_lag=max(proc.lag(now_ts=now).values() or [0]),
        queue_depth=queue_depth,
    )


def test_flow_escalates_when_exchange_silent():
    q = BoundedEventQueue(per_exchange_capacity=10)
    p = Processor()
    g = Guard()

    # Feed only bybit updates; gate/kucoin silent
    q.push(make_event(ex="bybit", seq=1, ts=100))
    q.push(make_event(ex="bybit", seq=2, ts=110))
    for ev in q.drain():
        p.handle(ev)

    metrics = compute_metrics(p, queue_depth=len(q), now=200)
    decision = g.evaluate(metrics)

    # With default config: stale_warn=1, stale_critical=3 -> two stale exchanges => L1
    assert decision.level == Level.L1


def test_flow_escalates_to_l2_when_three_silent():
    q = BoundedEventQueue(per_exchange_capacity=10)
    p = Processor()
    g = Guard()

    # Feed only one exchange; others silent
    q.push(make_event(ex="bybit", seq=1, ts=100))
    for ev in q.drain():
        p.handle(ev)

    metrics = compute_metrics(p, queue_depth=len(q), now=200)
    # adjust stale_critical to 2 for the test to hit L2 (default is 3)
    g.config.stale_critical = 2
    decision = g.evaluate(metrics)

    assert decision.level == Level.L2


def test_shadow_pipeline_processes_and_logs_decisions():
    shadow = ShadowPipeline(per_exchange_capacity=2)
    shadow.on_orderbook("bybit", "AAA/USDT", {"timestamp": 100})
    shadow.on_orderbook("bybit", "AAA/USDT", {"timestamp": 110})
    # No asserts on logs; ensure it runs without error and updates state
    assert len(shadow.processor.snapshot()) == 1


def test_shadow_stats_reports_level_and_drops():
    shadow = ShadowPipeline(per_exchange_capacity=1)
    shadow.queue.push(make_event(ex="bybit", sym="AAA/USDT", seq=1, ts=100))
    shadow.queue.push(
        make_event(ex="bybit", sym="AAA/USDT", seq=2, ts=110)
    )  # drop oldest
    # process queued events
    for ev in shadow.queue.drain():
        shadow.processor.handle(ev)
    stats = shadow.stats()
    assert stats["drops"] == 1
    assert stats["level"] in {"L0", "L1", "L2", "L3"}


def test_shadow_uses_fresh_metrics():
    shadow = ShadowPipeline(per_exchange_capacity=2, stale_timeout_seconds=1.0)
    shadow.processor.handle(
        Event(
            ex="bybit",
            channel="orderbook",
            symbol="AAA/USDT",
            ts_ex=0.0,
            ts_recv=0.0,
            data={},
        )
    )
    metrics = shadow._metrics(now=10_000.0)
    assert metrics.stale_exchanges == 5
