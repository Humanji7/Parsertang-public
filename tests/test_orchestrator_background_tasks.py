from parsertang.core.orchestrator import Orchestrator
from parsertang.core.state_manager import AppState


def test_background_tasks_include_validation_summary():
    orch = Orchestrator()
    orch.state = AppState()

    names: list[str] = []

    def fake_create_background_task(_coro, name: str):
        if hasattr(_coro, "close"):
            _coro.close()
        names.append(name)
        return object()

    orch._create_background_task = fake_create_background_task  # type: ignore[assignment]

    orch._start_background_tasks()

    assert "log_validation_summary" in names
