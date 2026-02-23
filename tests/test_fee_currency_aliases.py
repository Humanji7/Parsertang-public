from __future__ import annotations

import time

from parsertang.withdrawal_fees import (
    WithdrawalFeeCache,
    WithdrawalFeeManager,
    canonical_currency_codes,
)


def test_withdrawal_fee_currency_alias_apt_to_aptos() -> None:
    manager = WithdrawalFeeManager(exchanges={})
    manager.cache = WithdrawalFeeCache(
        fees={
            "mexc": {
                "APTOS": {
                    "APTOS": 0.24,
                },
            },
        },
        last_updated=time.time(),
    )

    fee, confidence = manager.get_withdrawal_fee("mexc", "APT", "APTOS")
    assert fee == 0.24
    assert confidence == "HIGH"


def test_canonical_currency_codes_uses_safe_currency_code() -> None:
    class FakeExchange:
        @staticmethod
        def safe_currency_code(code: str) -> str:
            if code == "APTOS":
                return "APT"
            return code

    assert canonical_currency_codes(FakeExchange(), "aptos") == ("APTOS", "APT")
