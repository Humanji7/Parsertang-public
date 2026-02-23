from __future__ import annotations

import asyncio

import pytest

from parsertang.alert_truth import AlertTruthEvidence, verify_alert_truth_after_delay
from parsertang.v2.validator import ValidationResult


@pytest.mark.asyncio
async def test_alert_truth_fails_on_fee_mismatch(monkeypatch):
    # Force strict tolerance for the test.
    from parsertang import config as config_mod

    monkeypatch.setattr(config_mod.settings, "alert_verify_fee_enabled", True)
    monkeypatch.setattr(config_mod.settings, "alert_verify_fee_tolerance_pct", 0.0)
    monkeypatch.setattr(config_mod.settings, "alert_verify_fee_tolerance_base", 0.0)
    monkeypatch.setattr(config_mod.settings, "alert_verify_delay_seconds", 0.0)

    class Ex:
        def fetch_currencies(self):
            return {
                "ALGO": {
                    "networks": {
                        "ALGO": {"withdraw": True, "active": True, "fee": 0.009},
                    }
                }
            }

    class Gateway:
        exchanges = {"okx": Ex()}

    class FakeValidator:
        gateway = Gateway()

        def validate(self, **kwargs):
            return ValidationResult(ok=True, reason="ok", rest_buy=1.0, rest_sell=1.0)

    evidence = AlertTruthEvidence(
        ts_wall=1.0,
        symbol="ALGO/USDT",
        buy_ex="okx",
        sell_ex="mexc",
        ws_buy=0.1314,
        ws_sell=0.1343,
        ws_ts_buy=1.0,
        ws_ts_sell=1.0,
        ws_age_buy_ms=0,
        ws_age_sell_ms=0,
        ws_skew_ms=0,
        net_profit_pct=0.2,
        network="ALGO",
        trade_fees_pct=0.15,
        withdraw_fee_pct=0.0,
        withdraw_fee_base=0.008,
        fee_cache_age_seconds=0.0,
    )

    res = await verify_alert_truth_after_delay(
        validator=FakeValidator(),  # type: ignore[arg-type]
        evidence=evidence,
        delay_seconds=0.0,
    )
    assert res.ok is False
    assert res.reason == "fee_mismatch"

