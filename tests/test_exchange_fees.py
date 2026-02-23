"""Tests for static exchange fees module."""

from parsertang.exchange_fees import get_taker_fee, get_maker_fee, get_fees


class TestExchangeFees:
    """Test static exchange fee lookups."""

    def test_bybit_fees(self):
        """Bybit: 0.10% taker, 0.10% maker."""
        assert get_taker_fee("bybit") == 0.10
        assert get_maker_fee("bybit") == 0.10
        assert get_fees("bybit") == (0.10, 0.10)

    def test_okx_fees(self):
        """OKX: 0.10% taker, 0.08% maker."""
        assert get_taker_fee("okx") == 0.10
        assert get_maker_fee("okx") == 0.08
        assert get_fees("okx") == (0.08, 0.10)

    def test_kucoin_fees(self):
        """KuCoin: 0.10% taker, 0.10% maker."""
        assert get_taker_fee("kucoin") == 0.10
        assert get_maker_fee("kucoin") == 0.10
        assert get_fees("kucoin") == (0.10, 0.10)

    def test_mexc_fees_critical(self):
        """MEXC: 0.05% taker, 0% maker (CRITICAL TEST - was 0.10% before)."""
        assert get_taker_fee("mexc") == 0.05, "MEXC taker fee must be 0.05%, not 0.10%!"
        assert get_maker_fee("mexc") == 0.00
        assert get_fees("mexc") == (0.00, 0.05)

    def test_htx_fees_critical(self):
        """HTX: 0.20% taker, 0.20% maker (CRITICAL TEST - was 0.10% before)."""
        assert get_taker_fee("htx") == 0.20, "HTX taker fee must be 0.20%, not 0.10%!"
        assert get_maker_fee("htx") == 0.20
        assert get_fees("htx") == (0.20, 0.20)

    def test_gate_fees(self):
        """Gate: 0.10% taker, 0.10% maker."""
        assert get_taker_fee("gate") == 0.10
        assert get_maker_fee("gate") == 0.10
        assert get_fees("gate") == (0.10, 0.10)

    def test_gate_alias(self):
        """Gate alias 'gateio' should work."""
        assert get_taker_fee("gateio") == 0.10
        assert get_maker_fee("gateio") == 0.10

    def test_unknown_exchange_default(self):
        """Unknown exchanges default to 0.10%/0.10%."""
        assert get_taker_fee("unknown_exchange") == 0.10
        assert get_maker_fee("unknown_exchange") == 0.10
        assert get_fees("unknown_exchange") == (0.10, 0.10)

    def test_case_insensitive(self):
        """Exchange names should be case-insensitive."""
        assert get_taker_fee("BYBIT") == 0.10
        assert get_taker_fee("Bybit") == 0.10
        assert get_taker_fee("OKX") == 0.10
        assert get_taker_fee("okx") == 0.10
