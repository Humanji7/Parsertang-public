"""Unit tests for orchestrator symbol filtering."""

from parsertang.core.orchestrator import Orchestrator


class _DummyGateway:
    @staticmethod
    def is_stable_quote(symbol: str) -> bool:
        return symbol.split("/")[1].upper() in {"USDT", "USDC"}


def test_filter_stable_symbols_excludes_inactive() -> None:
    orch = Orchestrator()
    orch.gateway = _DummyGateway()

    markets = {
        "ACTIVE/USDT": {"spot": True, "active": True},
        "INACTIVE/USDT": {"spot": True, "active": False},
        "ACTIVE/USDC": {"spot": True, "active": True},
    }

    result = orch._filter_stable_symbols(markets)
    assert "ACTIVE/USDT" in result
    assert "ACTIVE/USDC" in result
    assert "INACTIVE/USDT" not in result
