from parsertang.fee_truth import fee_within_tolerance


def test_fee_within_tolerance_accepts_exact_match():
    assert fee_within_tolerance(
        expected_fee_base=0.008,
        actual_fee_base=0.008,
        tolerance_pct=0.0,
        tolerance_base=0.0,
    )


def test_fee_within_tolerance_accepts_pct_tolerance():
    assert fee_within_tolerance(
        expected_fee_base=0.008,
        actual_fee_base=0.00815,  # +1.875%
        tolerance_pct=2.0,
        tolerance_base=0.0,
    )


def test_fee_within_tolerance_accepts_abs_tolerance():
    assert fee_within_tolerance(
        expected_fee_base=0.000001,
        actual_fee_base=0.0000014,
        tolerance_pct=0.0,
        tolerance_base=0.000001,
    )


def test_fee_within_tolerance_rejects_outside_tolerance():
    assert not fee_within_tolerance(
        expected_fee_base=0.008,
        actual_fee_base=0.009,
        tolerance_pct=2.0,
        tolerance_base=0.0,
    )

