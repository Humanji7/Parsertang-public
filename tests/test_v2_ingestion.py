from parsertang.v2.queue import BoundedEventQueue
from parsertang.v2.models import Event
from parsertang.v2.processor import Processor


def make_event(ex="gate", seq=0, ts_recv=None):
    return Event(
        ex=ex,
        channel="orderbook",
        symbol="AAA/USDT",
        ts_ex=seq * 10,
        ts_recv=ts_recv if ts_recv is not None else seq * 10,
        data={"seq": seq},
    )


def test_queue_drops_oldest_per_exchange():
    q = BoundedEventQueue(per_exchange_capacity=2)
    q.push(make_event(seq=1))
    q.push(make_event(seq=2))
    q.push(make_event(seq=3))  # should drop seq 1

    drained = q.drain()
    assert [e.data["seq"] for e in drained] == [2, 3]


def test_processor_drops_out_of_order_events():
    proc = Processor()
    newer = make_event(seq=2, ts_recv=200)
    older = make_event(seq=1, ts_recv=100)

    proc.handle(newer)
    proc.handle(older)  # should be ignored

    state = proc.snapshot()
    assert state[("gate", "AAA/USDT")].data["seq"] == 2


def test_queue_counts_drops_when_over_capacity():
    q = BoundedEventQueue(per_exchange_capacity=2)
    q.push(make_event(seq=1))
    q.push(make_event(seq=2))
    q.push(make_event(seq=3))
    q.push(make_event(seq=4))
    # capacity 2: should drop 2 oldest
    assert q.stats()["drops"] == 2
    drained = q.drain()
    assert [e.data["seq"] for e in drained] == [3, 4]


def test_processor_tracks_lag_and_best_values():
    proc = Processor()
    proc.handle(make_event(seq=1, ts_recv=100))
    proc.handle(make_event(seq=2, ts_recv=150))

    snap = proc.snapshot()
    entry = snap[("gate", "AAA/USDT")]
    assert entry.data["seq"] == 2
    # lag is now - ts_recv; simulate a now=200
    lag = proc.lag(now_ts=200)[("gate", "AAA/USDT")]
    assert lag == 50
