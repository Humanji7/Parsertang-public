import sys
from types import ModuleType

from parsertang.config import Settings

telegram_mod = ModuleType("telegram")
telegram_mod.Bot = object
telegram_constants = ModuleType("telegram.constants")
telegram_constants.ParseMode = object
telegram_ext = ModuleType("telegram.ext")
telegram_ext.Application = object
telegram_ext.CommandHandler = object

sys.modules.setdefault("telegram", telegram_mod)
sys.modules.setdefault("telegram.constants", telegram_constants)
sys.modules.setdefault("telegram.ext", telegram_ext)
sys.modules.setdefault("ccxt", ModuleType("ccxt"))

from parsertang.core.orchestrator import Orchestrator


def test_should_use_native_ws_respects_flag():
    orch = Orchestrator()
    settings = Settings(ws_native_enabled=True)
    assert orch._should_use_native_ws(settings) is True


def test_build_native_ws_clients_filters_unknown():
    orch = Orchestrator()
    clients = orch._build_native_ws_clients(["okx", "nope", "mexc"])
    assert set(clients.keys()) == {"okx", "mexc"}
