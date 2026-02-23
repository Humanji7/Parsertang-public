"""Unit tests for fee_utils module.

Tests the convert_usd_fee_to_base_coins() function which is critical
for accurate fee accounting in arbitrage cycles.
"""

import pytest
from parsertang.fee_utils import convert_usd_fee_to_base_coins


class TestConvertUsdFeeToBaseCoins:
    """Test suite for convert_usd_fee_to_base_coins() function."""

    def test_stablecoin_usdt_conversion(self):
        """Test USDT conversion (1:1 with USD)."""
        # $1 USD fee for USDT (stablecoin)
        result = convert_usd_fee_to_base_coins(1.0, "USDT", 1.0)
        assert result == 1.0, "USDT should convert 1:1 with USD"

    def test_stablecoin_usdc_conversion(self):
        """Test USDC conversion (1:1 with USD)."""
        # $5 USD fee for USDC (stablecoin)
        result = convert_usd_fee_to_base_coins(5.0, "USDC", 1.0)
        assert result == 5.0, "USDC should convert 1:1 with USD"

    def test_stablecoin_dai_conversion(self):
        """Test DAI conversion (1:1 with USD)."""
        # $0.50 USD fee for DAI (stablecoin)
        result = convert_usd_fee_to_base_coins(0.5, "DAI", 1.0)
        assert result == 0.5, "DAI should convert 1:1 with USD"

    def test_stablecoin_lowercase(self):
        """Test that lowercase stablecoin symbols work correctly."""
        # Function should uppercase the currency internally
        result = convert_usd_fee_to_base_coins(2.0, "usdt", 1.0)
        assert result == 2.0, "Lowercase 'usdt' should be handled correctly"

    def test_non_stablecoin_ltc_conversion(self):
        """Test LTC conversion at $100/LTC."""
        # $5 USD fee for LTC at $100/LTC
        # Expected: 5 / 100 = 0.05 LTC
        result = convert_usd_fee_to_base_coins(5.0, "LTC", 100.0)
        assert result == 0.05, "Should correctly convert USD to LTC"

    def test_non_stablecoin_btc_conversion(self):
        """Test BTC conversion at $50,000/BTC."""
        # $10 USD fee for BTC at $50,000/BTC
        # Expected: 10 / 50000 = 0.0002 BTC
        result = convert_usd_fee_to_base_coins(10.0, "BTC", 50000.0)
        assert result == 0.0002, "Should correctly convert USD to BTC"

    def test_non_stablecoin_eth_conversion(self):
        """Test ETH conversion at $3,000/ETH."""
        # $30 USD fee for ETH at $3,000/ETH
        # Expected: 30 / 3000 = 0.01 ETH
        result = convert_usd_fee_to_base_coins(30.0, "ETH", 3000.0)
        assert result == 0.01, "Should correctly convert USD to ETH"

    def test_zero_price_handling(self):
        """Test that zero price returns 0.0 and doesn't crash."""
        # Should return 0.0 instead of raising ZeroDivisionError
        result = convert_usd_fee_to_base_coins(10.0, "LTC", 0.0)
        assert result == 0.0, "Zero price should return 0.0"

    def test_negative_price_handling(self):
        """Test that negative price returns 0.0."""
        # Invalid negative price should be handled gracefully
        result = convert_usd_fee_to_base_coins(10.0, "LTC", -100.0)
        assert result == 0.0, "Negative price should return 0.0"

    def test_very_small_fee(self):
        """Test precision with very small fees (<$0.01)."""
        # $0.001 USD fee for BTC at $50,000
        # Expected: 0.001 / 50000 = 0.00000002 BTC (2 satoshis)
        result = convert_usd_fee_to_base_coins(0.001, "BTC", 50000.0)
        expected = 0.001 / 50000.0
        assert (
            abs(result - expected) < 1e-12
        ), "Should handle small fees with good precision"

    def test_very_large_fee(self):
        """Test handling of unrealistic large fees (>$1000)."""
        # $10,000 USD fee for ETH at $3,000
        # Expected: 10000 / 3000 = 3.333... ETH
        result = convert_usd_fee_to_base_coins(10000.0, "ETH", 3000.0)
        expected = 10000.0 / 3000.0
        assert abs(result - expected) < 1e-10, "Should handle large fees correctly"

    def test_zero_fee(self):
        """Test that zero fee returns 0.0."""
        result = convert_usd_fee_to_base_coins(0.0, "LTC", 100.0)
        assert result == 0.0, "Zero fee should return 0.0"

    def test_all_supported_stablecoins(self):
        """Test all stablecoins defined in STABLE_QUOTES."""
        stablecoins = ["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "EURC"]
        for stable in stablecoins:
            result = convert_usd_fee_to_base_coins(1.0, stable, 1.0)
            assert result == 1.0, f"{stable} should convert 1:1 with USD"

    def test_high_precision_price(self):
        """Test with high precision price values."""
        # $1 fee for a low-value coin at $0.00123456
        result = convert_usd_fee_to_base_coins(1.0, "DOGE", 0.00123456)
        expected = 1.0 / 0.00123456
        assert abs(result - expected) < 1e-6, "Should handle high precision prices"

    def test_realistic_ltc_scenario(self):
        """Test realistic LTC/USDT withdrawal scenario from the fix plan."""
        # Scenario: LTC/USDT pair
        # - Withdrawal fee: $5 USD (from BASELINE_NETWORKS for a network)
        # - LTC price: $100/USDT
        # - Expected: 0.05 LTC
        fee_usd = 5.0
        base_currency = "LTC"
        base_price = 100.0

        result = convert_usd_fee_to_base_coins(fee_usd, base_currency, base_price)
        assert result == 0.05, "LTC withdrawal fee conversion failed"

    def test_realistic_usdt_scenario(self):
        """Test realistic USDT/USDC withdrawal scenario."""
        # Scenario: USDT/USDC pair (both stablecoins)
        # - Withdrawal fee: $1 USD (TRC20 network)
        # - USDT price: ~$1.00
        # - Expected: 1.0 USDT
        fee_usd = 1.0
        base_currency = "USDT"
        base_price = 1.0

        result = convert_usd_fee_to_base_coins(fee_usd, base_currency, base_price)
        assert result == 1.0, "USDT withdrawal fee conversion failed"

    def test_realistic_btc_scenario(self):
        """Test realistic BTC withdrawal with high value."""
        # Scenario: BTC/USDT pair
        # - Withdrawal fee: $10 USD (typical for BTC network)
        # - BTC price: $65,000/USDT
        # - Expected: 0.00015384... BTC
        fee_usd = 10.0
        base_currency = "BTC"
        base_price = 65000.0

        result = convert_usd_fee_to_base_coins(fee_usd, base_currency, base_price)
        expected = 10.0 / 65000.0  # ~0.0001538
        assert abs(result - expected) < 1e-10, "BTC withdrawal fee conversion failed"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_mixed_case_currency_symbol(self):
        """Test that mixed case currency symbols work."""
        # Test "Usdt" (mixed case)
        result = convert_usd_fee_to_base_coins(1.0, "Usdt", 1.0)
        assert result == 1.0, "Mixed case 'Usdt' should be handled"

    def test_very_high_price(self):
        """Test with extremely high price (e.g., rare collectible token)."""
        # $1 fee for a token worth $1,000,000
        result = convert_usd_fee_to_base_coins(1.0, "RARE", 1000000.0)
        expected = 1.0 / 1000000.0  # 0.000001
        assert result == expected, "Should handle very high prices"

    def test_very_low_price(self):
        """Test with extremely low price (e.g., micro-cap token)."""
        # $1 fee for a token worth $0.0000001
        result = convert_usd_fee_to_base_coins(1.0, "MICRO", 0.0000001)
        expected = 1.0 / 0.0000001  # 10,000,000
        assert abs(result - expected) < 1e-3, "Should handle very low prices"

    def test_price_exactly_one(self):
        """Test with price exactly 1.0 for non-stablecoin."""
        # Non-stablecoin (not in STABLE_QUOTES) but priced at $1
        result = convert_usd_fee_to_base_coins(5.0, "SOME_TOKEN", 1.0)
        assert result == 5.0, "Should convert correctly even when price is 1.0"

    def test_floating_point_precision(self):
        """Test that floating point arithmetic is handled correctly."""
        # Test case that might expose floating point issues
        fee_usd = 0.1  # 10 cents
        base_price = 0.3  # 30 cents
        result = convert_usd_fee_to_base_coins(fee_usd, "TOKEN", base_price)
        expected = 0.1 / 0.3  # Should be 0.333...
        # Use relative tolerance due to floating point
        assert (
            abs(result - expected) / expected < 1e-10
        ), "Should handle floating point correctly"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
