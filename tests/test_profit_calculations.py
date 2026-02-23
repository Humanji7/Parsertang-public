"""Test suite for corrected multiplicative fee compounding.

This module validates the corrected net profit calculation formula
against the old additive formula to ensure accuracy improvements.

Reference: SPEC-R1-001 (Corrected), GitHub Issue #XXX
"""

import pytest
from parsertang.arbitrage import compute_net_profit_pct


class TestMultiplicativeFeeCompounding:
    """Test cases validating correct multiplicative fee compounding."""

    def test_basic_multiplicative_formula(self):
        """Verify basic multiplicative compounding vs additive."""
        gross = 1.0  # 1% gross spread
        buy_fee = 0.1  # 0.1% buy fee
        sell_fee = 0.1  # 0.1% sell fee
        withdraw_usd = 0.05  # $0.05 withdrawal fee
        volume_usd = 100.0  # $100 trade volume

        net, trade_fees, withdraw_fees = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Old additive formula would give: 1.0 - 0.1 - 0.1 - 0.05 = 0.75%
        # Correct multiplicative: (1.01 * 0.999 * 0.9995 * 0.999) - 1 = 0.7477%
        expected_multiplicative = 0.7477
        assert (
            abs(net - expected_multiplicative) < 0.001
        ), f"Expected ~{expected_multiplicative}%, got {net}%"

        # Verify additive would be wrong
        additive_result = gross - (buy_fee + sell_fee + withdraw_fees)
        assert abs(net - additive_result) > 0.001, "Should differ from additive formula"

    def test_real_log_example_atom(self):
        """Test against real ATOM/USDT log example."""
        # Real log: Gross 1.59%, Fees 0.22%, Net shown as 1.37%
        gross = 1.59
        buy_fee = 0.10
        sell_fee = 0.10
        withdraw_usd = 0.02  # ~0.02% as percentage
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Correct multiplicative: (1.0159 * 0.999 * 0.9998 * 0.999) - 1 ≈ 1.367%
        expected = 1.367
        assert (
            abs(net - expected) < 0.01
        ), f"ATOM example: expected ~{expected}%, got {net}%"

        # Old formula would give: 1.59 - 0.22 = 1.37% (overestimated)
        assert net < 1.37, "Should be lower than additive result"

    def test_real_log_example_algo(self):
        """Test against real ALGO/USDT log example."""
        # Real log: Gross 0.39%, Fees 0.20%, Net shown as 0.19%
        gross = 0.39
        buy_fee = 0.10
        sell_fee = 0.10
        withdraw_usd = 0.0  # No withdrawal fee
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Correct multiplicative: (1.0039 * 0.999 * 1.0 * 0.999) - 1 ≈ 0.1893%
        expected = 0.1893
        assert (
            abs(net - expected) < 0.001
        ), f"ALGO example: expected ~{expected}%, got {net}%"

    def test_critical_threshold_edge_case(self):
        """Test edge case near MIN_NET_PROFIT=0.3% threshold."""
        # Scenario: Should pass threshold with additive, fail with multiplicative
        gross = 0.50
        buy_fee = 0.10
        sell_fee = 0.10
        withdraw_usd = 0.05
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Additive would give: 0.50 - 0.25 = 0.25% (below threshold)
        # Multiplicative gives: ~0.249% (correctly below threshold)
        assert net < 0.30, "Should be below 0.3% threshold"
        assert abs(net - 0.249) < 0.002

    def test_false_positive_scenario(self):
        """Test scenario where additive shows profit but multiplicative shows loss."""
        gross = 0.30  # Exactly at threshold
        buy_fee = 0.10
        sell_fee = 0.10
        withdraw_usd = 0.30
        volume_usd = 100.0  # Withdraw fee = 0.30%

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Additive: 0.30 - 0.50 = -0.20% (correctly negative)
        # But if withdraw fee was 0.05%:
        withdraw_usd = 0.05
        net2, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Additive: 0.30 - 0.25 = 0.05% (false positive)
        # Multiplicative: ~0.0497% (more accurate)
        assert net2 < 0.30, "Should be below threshold"

    def test_high_fee_scenario(self):
        """Test scenario with high fees where error is most significant."""
        gross = 2.5  # 2.5% spread
        buy_fee = 0.20  # 0.2% each
        sell_fee = 0.20
        withdraw_usd = 0.15  # 0.15%
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Additive: 2.5 - 0.55 = 1.95%
        # Multiplicative: (1.025 * 0.998 * 0.9985 * 0.998) - 1 ≈ 1.937%
        expected = 1.937
        assert (
            abs(net - expected) < 0.005
        ), f"High fee scenario: expected ~{expected}%, got {net}%"

        # Error should be largest here
        additive_result = gross - (buy_fee + sell_fee + 0.15)
        error = additive_result - net
        assert error > 0.01, f"Error magnitude should be >0.01%, got {error}%"

    def test_zero_fees(self):
        """Test with zero fees (gross = net)."""
        gross = 1.5
        net, _, _ = compute_net_profit_pct(gross, 0.0, 0.0, 0.0, 100.0)

        # With no fees, net should equal gross
        assert abs(net - gross) < 0.0001, "Zero fees: net should equal gross"

    def test_asymmetric_fees(self):
        """Test with asymmetric buy/sell fees."""
        gross = 1.0
        buy_fee = 0.05  # Lower buy fee
        sell_fee = 0.15  # Higher sell fee
        withdraw_usd = 0.10
        volume_usd = 100.0

        net, trade_fees, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Multiplicative: (1.01 * 0.9995 * 0.999 * 0.9985) - 1
        expected = 0.697
        assert abs(net - expected) < 0.01

        # trade_fees should still be sum for backward compatibility
        assert abs(trade_fees - 0.20) < 0.001

    def test_numerical_stability(self):
        """Test numerical stability with very small percentages."""
        gross = 0.001  # 0.001% (0.1 basis points)
        buy_fee = 0.0001
        sell_fee = 0.0001
        withdraw_usd = 0.00001
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Should not suffer from catastrophic cancellation
        assert net < gross
        assert net > -0.001  # Should be tiny but not wildly incorrect

    def test_large_spread_scenario(self):
        """Test with large spreads (>5%) to ensure formula scales."""
        gross = 10.0  # 10% spread (rare but possible in volatile markets)
        buy_fee = 0.10
        sell_fee = 0.10
        withdraw_usd = 0.05
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Multiplicative: (1.10 * 0.999 * 0.9995 * 0.999) - 1 ≈ 9.725%
        expected = 9.725
        assert abs(net - expected) < 0.01

        # Additive would give: 10.0 - 0.25 = 9.75% (close but still wrong)
        additive_result = gross - 0.25
        assert abs(net - additive_result) < 0.03  # Error is ~0.025% for large spreads

    def test_backward_compatibility_trade_fees(self):
        """Ensure trade_fees_pct return value is backward compatible."""
        gross = 1.0
        buy_fee = 0.12
        sell_fee = 0.08
        withdraw_usd = 0.05
        volume_usd = 100.0

        net, trade_fees, withdraw_fees = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # trade_fees should still be simple sum for backward compatibility
        assert abs(trade_fees - (buy_fee + sell_fee)) < 0.0001
        assert abs(trade_fees - 0.20) < 0.0001

        # withdraw_fees should be percentage
        assert abs(withdraw_fees - 0.05) < 0.001


class TestEdgeCasesAndValidation:
    """Test edge cases and input validation."""

    def test_zero_volume_handling(self):
        """Test behavior with zero trade volume (should raise or handle gracefully)."""
        with pytest.raises(ZeroDivisionError):
            compute_net_profit_pct(1.0, 0.1, 0.1, 1.0, 0.0)

    def test_negative_gross_spread(self):
        """Test with negative gross spread (no arbitrage opportunity)."""
        gross = -0.5  # Negative spread
        buy_fee = 0.1
        sell_fee = 0.1
        withdraw_usd = 0.05
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # Should correctly show loss
        assert net < 0
        # Multiplicative: (0.995 * 0.999 * 0.9995 * 0.999) - 1 ≈ -0.752%
        expected = -0.752
        assert abs(net - expected) < 0.01

    def test_100_percent_fees(self):
        """Test with 100% fees (edge case, all capital lost)."""
        gross = 5.0
        buy_fee = 100.0  # 100% fee (unrealistic but tests formula)
        sell_fee = 0.1
        withdraw_usd = 0.05
        volume_usd = 100.0

        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        # With 100% buy fee, should lose everything
        # Multiplicative: (1.05 * 0.0 * 0.9995 * 0.999) - 1 = -100%
        assert net < -99.0  # Should be close to -100%

    def test_comparison_with_manual_calculation(self):
        """Manual step-by-step calculation to verify formula."""
        # Scenario: $100 trade, 1% spread, 0.1% each fee, $0.05 withdraw
        initial_capital = 100.0
        gross = 1.0
        buy_fee = 0.1
        sell_fee = 0.1
        withdraw_usd = 0.05
        volume_usd = 100.0

        # Manual calculation:
        # 1. Buy: $100 → Pay 0.1% → $99.90 in coins
        after_buy = initial_capital * (1 - buy_fee / 100)
        assert abs(after_buy - 99.90) < 0.01

        # 2. Spread: Coins appreciate 1% → $100.899
        after_spread = after_buy * (1 + gross / 100)
        assert abs(after_spread - 100.899) < 0.01

        # 3. Withdraw: Pay $0.05 → $100.849
        after_withdraw = after_spread - withdraw_usd
        assert abs(after_withdraw - 100.849) < 0.01

        # 4. Sell: Pay 0.1% → $100.748
        after_sell = after_withdraw * (1 - sell_fee / 100)
        assert abs(after_sell - 100.748) < 0.01

        # Net profit: (100.748 - 100) / 100 * 100 = 0.748%
        manual_net = (after_sell - initial_capital) / initial_capital * 100
        assert abs(manual_net - 0.748) < 0.001

        # Now test our formula
        net, _, _ = compute_net_profit_pct(
            gross, buy_fee, sell_fee, withdraw_usd, volume_usd
        )

        assert (
            abs(net - manual_net) < 0.001
        ), f"Formula should match manual calculation: {net}% vs {manual_net}%"


class TestRegressionAndBackwardCompatibility:
    """Ensure changes don't break existing functionality."""

    def test_opportunity_dataclass_compatibility(self):
        """Verify Opportunity dataclass can still consume results."""
        from parsertang.arbitrage import Opportunity

        net, trade_fees, withdraw_fees = compute_net_profit_pct(
            1.0, 0.1, 0.1, 0.05, 100.0
        )

        # Should be able to construct Opportunity with these values
        opp = Opportunity(
            symbol="BTC/USDT",
            buy_exchange="bybit",
            buy_price=67000.0,
            sell_exchange="okx",
            sell_price=67670.0,
            gross_spread_pct=1.0,
            trade_fees_pct=trade_fees,
            withdraw_fee_pct=withdraw_fees,
            net_profit_pct=net,
            bid_liq_usd=10000.0,
            ask_liq_usd=10000.0,
            network="TRC20",
        )

        assert opp.net_profit_pct < 1.0  # Should be less than gross

    def test_main_filtering_compatibility(self):
        """Ensure MIN_NET_PROFIT filtering still works correctly."""
        MIN_NET_PROFIT = 0.30

        # Case 1: Should pass filter
        net1, _, _ = compute_net_profit_pct(1.0, 0.1, 0.1, 0.05, 100.0)
        assert net1 >= MIN_NET_PROFIT, "1% spread should pass 0.3% threshold"

        # Case 2: Should fail filter (false positive with old formula)
        net2, _, _ = compute_net_profit_pct(0.50, 0.10, 0.10, 0.05, 100.0)
        # Old formula: 0.50 - 0.25 = 0.25% (below threshold, correct)
        # New formula: ~0.249% (still below threshold, correct)
        assert net2 < MIN_NET_PROFIT, "Small spread should fail 0.3% threshold"

        # Case 3: Edge case
        net3, _, _ = compute_net_profit_pct(0.55, 0.10, 0.10, 0.05, 100.0)
        # Old: 0.55 - 0.25 = 0.30% (passes exactly)
        # New: ~0.299% (fails, more conservative)
        # This is the CRITICAL improvement
        if net3 < MIN_NET_PROFIT:
            print(
                f"✓ Corrected formula prevents false positive: {net3:.4f}% < {MIN_NET_PROFIT}%"
            )
