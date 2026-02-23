from __future__ import annotations

from parsertang.withdrawal_fees import (
    extract_withdraw_fee_from_currencies,
    extract_withdraw_fee_from_deposit_withdraw_fees,
    fetch_withdraw_fee_live,
    normalize_network_code,
)


def test_normalize_network_code_strips_suffix():
    assert normalize_network_code("TRC20-USDT", "USDT") == "TRC20"
    assert normalize_network_code("USDT-TRC20", "USDT") == "TRC20"


def test_extract_withdraw_fee_from_currencies():
    currencies = {
        "ALGO": {
            "networks": {
                "ALGO": {"withdraw": True, "active": True, "fee": 0.008},
            }
        }
    }
    assert (
        extract_withdraw_fee_from_currencies(
            currencies,
            currency="ALGO",
            network="ALGO",
        )
        == 0.008
    )


def test_extract_withdraw_fee_from_deposit_withdraw_fees():
    fees = {
        "ALGO": {
            "networks": {
                "ALGO": {"withdraw": {"fee": 0.008}},
            }
        }
    }
    assert (
        extract_withdraw_fee_from_deposit_withdraw_fees(
            fees,
            currency="ALGO",
            network="ALGO",
        )
        == 0.008
    )


def test_fetch_withdraw_fee_live_prefers_currencies():
    class Ex:
        def fetch_currencies(self):
            return {
                "ALGO": {
                    "networks": {
                        "ALGO": {"withdraw": True, "active": True, "fee": 0.008},
                    }
                }
            }

        def fetch_deposit_withdraw_fees(self):
            raise AssertionError("should not be called")

    fee, src = fetch_withdraw_fee_live(Ex(), currency="ALGO", network="ALGO")
    assert fee == 0.008
    assert src == "currencies"


def test_fetch_withdraw_fee_live_falls_back_to_deposit_withdraw_fees():
    class Ex:
        def fetch_currencies(self):
            return {"ALGO": {"networks": {"ALGO": {"withdraw": True, "active": True, "fee": None}}}}

        def fetch_deposit_withdraw_fees(self):
            return {"ALGO": {"networks": {"ALGO": {"withdraw": {"fee": 0.008}}}}}

    fee, src = fetch_withdraw_fee_live(Ex(), currency="ALGO", network="ALGO")
    assert fee == 0.008
    assert src == "deposit_withdraw_fees"

