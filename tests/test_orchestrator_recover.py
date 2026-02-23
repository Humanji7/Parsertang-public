import asyncio
import sys
import types

import pytest


def _install_telegram_stubs() -> None:
    telegram = types.ModuleType("telegram")
    telegram_constants = types.ModuleType("telegram.constants")
    telegram_ext = types.ModuleType("telegram.ext")

    class _Bot:  # minimal stub
        def __init__(self, *args, **kwargs) -> None:
            pass

    class _Application:  # minimal stub
        @classmethod
        def builder(cls):
            return cls()

        def token(self, *args, **kwargs):
            return self

        def build(self):
            return self

        def add_handler(self, *args, **kwargs):
            return None

        async def start(self):
            return None

        async def stop(self):
            return None

        async def initialize(self):
            return None

        async def shutdown(self):
            return None

    class _CommandHandler:  # minimal stub
        def __init__(self, *args, **kwargs) -> None:
            pass

    telegram.Bot = _Bot
    telegram_constants.ParseMode = types.SimpleNamespace(HTML="HTML")
    telegram_ext.Application = _Application
    telegram_ext.CommandHandler = _CommandHandler

    sys.modules.setdefault("telegram", telegram)
    sys.modules.setdefault("telegram.constants", telegram_constants)
    sys.modules.setdefault("telegram.ext", telegram_ext)


_install_telegram_stubs()
sys.modules.setdefault("ccxt", types.ModuleType("ccxt"))

from parsertang.core.orchestrator import Orchestrator  # noqa: E402
from parsertang.core.state_manager import AppState  # noqa: E402
from parsertang.refresh_trace import RefreshTrace  # noqa: E402


@pytest.mark.asyncio
async def test_request_ws_recover_returns_true_when_completed():
    orch = Orchestrator()

    task = asyncio.create_task(orch.request_ws_recover(timeout_seconds=0.2))
    await asyncio.sleep(0)

    assert orch._ws_recover_event.is_set() is True
    assert orch._ws_recover_future is not None
    orch._ws_recover_future.set_result(True)

    result = await task

    assert result is True


class _AlertStub:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_tech(self, text: str) -> None:
        self.messages.append(text)


@pytest.mark.asyncio
async def test_refresh_trace_report_not_sent_without_start():
    orch = Orchestrator()
    orch.state = AppState()
    orch.state.alert_service = _AlertStub()
    orch._refresh_trace = RefreshTrace(reason="l1_recover", start_ts=100.0)
    orch._refresh_trace_reported = False

    orch._report_refresh_trace(asyncio.get_running_loop())
    await asyncio.sleep(0)

    assert orch.state.alert_service.messages == []
    assert orch._refresh_trace_reported is False


class _DummyClient:
    def __init__(self, ex_id: str) -> None:
        self.ex_id = ex_id
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1


class _DummyStreams:
    def __init__(self) -> None:
        self.created: list[str] = []
        self.started: list[str] = []

    def create_exchange(self, ex_id: str, proxy_config=None):  # type: ignore[override]
        self.created.append(ex_id)
        return _DummyClient(ex_id)

    def start_exchange_worker(self, ex_id, client, symbols, on_update):  # type: ignore[override]
        self.started.append(ex_id)

        async def worker():
            await asyncio.Event().wait()

        async def fire_update():
            await asyncio.sleep(0)
            on_update(ex_id, symbols[0], {"bids": [[1, 1]], "asks": [[1.1, 1]]})

        task = asyncio.create_task(worker())
        asyncio.create_task(fire_update())
        return task


@pytest.mark.asyncio
async def test_l0_restart_exchange_does_not_touch_others():
    orch = Orchestrator()
    orch.state = AppState()
    orch.streams = _DummyStreams()
    orch._ws_on_update = lambda ex_id, symbol, ob: None
    orch._ws_symbols_per_exchange = {"bybit": ["BTC/USDT"], "okx": ["ETH/USDT"]}

    bybit_client = _DummyClient("bybit")
    okx_client = _DummyClient("okx")
    orch._ws_clients = {"bybit": bybit_client, "okx": okx_client}

    bybit_task = asyncio.create_task(asyncio.Event().wait())
    okx_task = asyncio.create_task(asyncio.Event().wait())
    orch._ws_tasks = {"bybit": bybit_task, "okx": okx_task}

    async with orch.state.orderbooks_lock:
        orch.state.orderbooks[("bybit", "BTC/USDT")] = object()
        orch.state.orderbooks[("okx", "ETH/USDT")] = object()

    async with orch.state.metrics_lock:
        orch.state.ws_metrics.update_counters["bybit"] = 5
        orch.state.ws_metrics.update_counters["okx"] = 7
        orch.state.ws_metrics.symbols_seen["bybit"].add("BTC/USDT")
        orch.state.ws_metrics.symbols_seen["okx"].add("ETH/USDT")
        orch.state.ws_metrics.stale_intervals["bybit"] = 2
        orch.state.ws_metrics.stale_intervals["okx"] = 3

    result = await orch.restart_exchange("bybit", timeout_seconds=0.5)

    assert result is True
    assert bybit_client.closed == 1
    assert okx_client.closed == 0
    assert bybit_task.cancelled() is True
    assert okx_task.cancelled() is False

    async with orch.state.orderbooks_lock:
        assert ("bybit", "BTC/USDT") not in orch.state.orderbooks
        assert ("okx", "ETH/USDT") in orch.state.orderbooks

    async with orch.state.metrics_lock:
        assert orch.state.ws_metrics.update_counters.get("bybit", 0) == 0
        assert orch.state.ws_metrics.update_counters.get("okx", 0) == 7
        assert "BTC/USDT" not in orch.state.ws_metrics.symbols_seen.get("bybit", set())
        assert "ETH/USDT" in orch.state.ws_metrics.symbols_seen.get("okx", set())
        assert orch.state.ws_metrics.stale_intervals.get("bybit", 0) == 0
        assert orch.state.ws_metrics.stale_intervals.get("okx", 0) == 3

    okx_task.cancel()
    await asyncio.gather(okx_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_l0_restart_exchange_logs_start_and_first_update(caplog):
    orch = Orchestrator()
    orch.state = AppState()
    orch.streams = _DummyStreams()
    orch._ws_on_update = lambda ex_id, symbol, ob: None
    orch._ws_symbols_per_exchange = {"bybit": ["BTC/USDT"]}
    orch._ws_clients = {"bybit": _DummyClient("bybit")}
    orch._ws_tasks = {"bybit": asyncio.create_task(asyncio.Event().wait())}

    with caplog.at_level("INFO"):
        result = await orch.restart_exchange(
            "bybit", timeout_seconds=0.5, reason="stale_exchanges"
        )

    assert result is True
    assert "L0 RECOVER | start ex=bybit reason=stale_exchanges" in caplog.text
    assert "L0 RECOVER | first_update ex=bybit" in caplog.text


@pytest.mark.asyncio
async def test_create_ws_gather_task_completes_all_workers():
    orch = Orchestrator()

    async def worker():
        await asyncio.sleep(0)

    t1 = asyncio.create_task(worker())
    t2 = asyncio.create_task(worker())

    gather_task = orch._create_ws_gather_task([t1, t2])
    await gather_task

    assert t1.done() is True
    assert t2.done() is True


@pytest.mark.asyncio
async def test_l1_recover_requires_expected_exchange_update(monkeypatch):
    import parsertang.core.orchestrator as orchestrator_module

    def _noop_on_orderbook_update(**_kwargs):
        return None

    monkeypatch.setattr(
        orchestrator_module, "on_orderbook_update", _noop_on_orderbook_update
    )

    orch = Orchestrator()
    orch.state = AppState()
    loop = asyncio.get_running_loop()
    callback = orch._create_orderbook_callback(loop)

    orch._ws_recover_future = loop.create_future()
    orch._ws_recover_expected_exchanges = {"bybit"}

    callback("okx", "BTC/USDT", {"bids": [[1, 1]], "asks": [[1.1, 1]]})
    assert orch._ws_recover_future.done() is False

    callback("bybit", "BTC/USDT", {"bids": [[1, 1]], "asks": [[1.1, 1]]})
    assert orch._ws_recover_future.done() is True
    assert orch._ws_recover_future.result() is True
    assert orch._ws_recover_expected_exchanges == set()
