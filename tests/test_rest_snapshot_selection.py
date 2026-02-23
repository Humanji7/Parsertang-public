from unittest.mock import patch

from parsertang.core.orchestrator import Orchestrator


class _GatewayStub:
    def is_stable_quote(self, symbol: str) -> bool:
        try:
            _base, quote = symbol.split("/")
        except ValueError:
            return False
        return quote.upper() in {"USDT", "USDC"}


def _market(volume: float) -> dict:
    return {"spot": True, "info": {"quoteVolume": volume}}


def test_select_rest_snapshot_symbols_excludes_ws_and_bases():
    orch = Orchestrator()
    orch.gateway = _GatewayStub()

    markets = {
        "kucoin": {
            "BTC/USDT": _market(1000),
            "ETH/USDT": _market(900),
            "SOL/USDT": _market(800),
            "XRP/USDT": _market(700),
            "BNB/USDT": _market(600),
            "TRX/USDT": _market(500),
            "ADA/USDT": _market(100),
            "DOGE/USDT": _market(90),
            "AVAX/USDT": _market(80),
            "DOT/USDT": _market(70),
        }
    }

    ws_symbols = {"kucoin": ["ADA/USDT"]}

    with patch("parsertang.config.settings.rest_snapshot_enabled", True):
        with patch("parsertang.config.settings.rest_snapshot_exchanges", ["kucoin"]):
            with patch("parsertang.config.settings.rest_snapshot_max_symbols", 2):
                selected = orch._select_rest_snapshot_symbols(
                    markets=markets,
                    ws_symbols_per_exchange=ws_symbols,
                )

    assert selected["kucoin"] == ["DOGE/USDT", "AVAX/USDT"]
