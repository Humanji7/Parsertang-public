"""
Test that withdrawal fees are correctly converted from base currency to USD.
Regression test for bug where withdrawal_fee_usd (actually in base currency)
was used directly in percentage calculation instead of being converted to USD.
"""

from parsertang.arbitrage import compute_net_profit_pct


def test_withdrawal_fee_conversion_zen_example():
    """Test ZEN withdrawal fee: 0.1 ZEN @ $13.30 = $1.33 USD should be 1.33% of $100 trade."""
    # Market data
    zen_price_usd = 13.30  # ZEN trading at $13.30
    withdrawal_fee_zen = 0.1  # Exchange charges 0.1 ZEN withdrawal fee
    trade_volume_usd = 100.0

    # Expected: 0.1 ZEN * $13.30 = $1.33 USD -> 1.33% of $100 trade
    expected_withdraw_fee_pct = 1.33

    # Simulate correct conversion: base currency -> USD
    withdrawal_fee_usd_converted = (
        withdrawal_fee_zen * zen_price_usd
    )  # 0.1 * 13.30 = 1.33 USD

    # Calculate profit with converted fee
    gross_spread_pct = 2.0  # 2% gross spread
    buy_fee = 0.1  # 0.1% buy fee
    sell_fee = 0.1  # 0.1% sell fee

    net_profit_pct, trade_fees_pct, withdraw_fee_pct = compute_net_profit_pct(
        gross_spread_pct,
        buy_fee,
        sell_fee,
        withdrawal_fee_usd_converted,  # Pass USD-converted value
        trade_volume_usd,
    )

    # Verify withdrawal fee percentage is correct
    assert (
        abs(withdraw_fee_pct - expected_withdraw_fee_pct) < 0.01
    ), f"Expected {expected_withdraw_fee_pct}%, got {withdraw_fee_pct}%"

    # Verify net profit (multiplicative formula)
    # Formula: (1 + gross/100) * (1 - buy/100) * (1 - withdraw/100) * (1 - sell/100) - 1
    # = (1.02) * (0.999) * (0.9867) * (0.999) - 1
    # = 1.0044216... - 1 = 0.0044216 = 0.44216%
    expected_net_profit_multiplicative = (1.02 * 0.999 * (1 - 0.0133) * 0.999 - 1) * 100
    assert (
        abs(net_profit_pct - expected_net_profit_multiplicative) < 0.01
    ), f"Expected ~{expected_net_profit_multiplicative:.3f}% net profit, got {net_profit_pct}%"


def test_withdrawal_fee_conversion_bug_demonstration():
    """Demonstrate the BUG: using base currency directly gives wrong result."""
    zen_price_usd = 13.30
    withdrawal_fee_zen = 0.1  # 0.1 ZEN
    trade_volume_usd = 100.0

    # BUG: Using base currency amount directly (without conversion to USD)
    # This was the bug in the original code
    net_profit_pct_wrong, _, withdraw_fee_pct_wrong = compute_net_profit_pct(
        2.0,  # 2% gross spread
        0.1,  # buy fee
        0.1,  # sell fee
        withdrawal_fee_zen,  # BUG: passing 0.1 ZEN as if it were $0.1 USD!
        trade_volume_usd,
    )

    # BUG result: 0.1 / 100 * 100 = 0.1% (WRONG!)
    assert (
        abs(withdraw_fee_pct_wrong - 0.1) < 0.01
    ), f"Bug gives {withdraw_fee_pct_wrong}%, should be 1.33%"

    # CORRECT: Convert to USD first
    withdrawal_fee_usd_converted = withdrawal_fee_zen * zen_price_usd  # 1.33 USD
    net_profit_pct_correct, _, withdraw_fee_pct_correct = compute_net_profit_pct(
        2.0,
        0.1,
        0.1,
        withdrawal_fee_usd_converted,  # CORRECT: 1.33 USD
        trade_volume_usd,
    )

    # Correct result: 1.33 / 100 * 100 = 1.33%
    assert (
        abs(withdraw_fee_pct_correct - 1.33) < 0.01
    ), f"Fixed version gives {withdraw_fee_pct_correct}%"

    # Verify significant difference between bug and fix
    difference_pct = abs(withdraw_fee_pct_correct - withdraw_fee_pct_wrong)
    assert (
        difference_pct > 1.0
    ), f"Bug causes {difference_pct:.2f}% error in withdrawal fee calculation!"


def test_withdrawal_fee_small_amounts():
    """Test that small withdrawal fees (< $1) are correctly calculated."""
    # LTC example: 0.0069 LTC @ $100 = $0.69 USD -> 0.69% of $100 trade
    ltc_price_usd = 100.0
    withdrawal_fee_ltc = 0.0069  # 0.0069 LTC
    trade_volume_usd = 100.0

    withdrawal_fee_usd_converted = withdrawal_fee_ltc * ltc_price_usd  # 0.69 USD

    _, _, withdraw_fee_pct = compute_net_profit_pct(
        1.0,  # 1% gross spread
        0.1,  # buy fee
        0.1,  # sell fee
        withdrawal_fee_usd_converted,
        trade_volume_usd,
    )

    expected_withdraw_fee_pct = 0.69
    assert (
        abs(withdraw_fee_pct - expected_withdraw_fee_pct) < 0.01
    ), f"Expected {expected_withdraw_fee_pct}%, got {withdraw_fee_pct}%"


def test_withdrawal_fee_high_value_coins():
    """Test withdrawal fees for high-value coins where difference is most visible."""
    # BTC example: 0.0005 BTC @ $50,000 = $25 USD -> 25% of $100 trade (unprofitable!)
    btc_price_usd = 50000.0
    withdrawal_fee_btc = 0.0005  # 0.0005 BTC
    trade_volume_usd = 100.0

    withdrawal_fee_usd_converted = withdrawal_fee_btc * btc_price_usd  # 25 USD

    net_profit_pct, _, withdraw_fee_pct = compute_net_profit_pct(
        30.0,  # Even with 30% gross spread
        0.1,  # buy fee
        0.1,  # sell fee
        withdrawal_fee_usd_converted,
        trade_volume_usd,
    )

    # Withdrawal fee: 25 / 100 * 100 = 25%
    assert (
        abs(withdraw_fee_pct - 25.0) < 0.01
    ), f"Expected 25% withdrawal fee, got {withdraw_fee_pct}%"

    # Net profit (multiplicative formula)
    # Formula: (1 + gross/100) * (1 - buy/100) * (1 - withdraw/100) * (1 - sell/100) - 1
    # = (1.30) * (0.999) * (0.75) * (0.999) - 1
    # = 0.973... - 1 = -0.0269... = -2.69%
    expected_net_profit_multiplicative = (1.30 * 0.999 * 0.75 * 0.999 - 1) * 100
    assert (
        abs(net_profit_pct - expected_net_profit_multiplicative) < 0.1
    ), f"Expected ~{expected_net_profit_multiplicative:.2f}% net profit (LOSS), got {net_profit_pct}%"

    # The multiplicative formula correctly shows this is unprofitable!
    # OLD additive: 30% - 0.1% - 0.1% - 25% = 4.8% (WRONG!)
    # WITHOUT conversion (BUG), would show: 0.0005 / 100 * 100 = 0.0005% withdrawal fee
    # Leading to: 30% - 0.1% - 0.1% - 0.0005% = 29.7995% net profit (VERY OPTIMISTIC!)
