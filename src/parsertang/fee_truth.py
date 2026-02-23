from __future__ import annotations

import math


def fee_within_tolerance(
    *,
    expected_fee_base: float,
    actual_fee_base: float,
    tolerance_pct: float,
    tolerance_base: float,
) -> bool:
    """Compare withdrawal fees in base currency units with mixed tolerance.

    We use both:
    - relative tolerance (percent of expected)
    - absolute tolerance (base currency units)

    This handles both tiny-fee assets (need an abs floor) and large-fee assets
    (need relative tolerance).
    """
    if not all(
        isinstance(x, (int, float))
        for x in (
            expected_fee_base,
            actual_fee_base,
            tolerance_pct,
            tolerance_base,
        )
    ):
        return False
    if not all(
        math.isfinite(float(x))
        for x in (
            expected_fee_base,
            actual_fee_base,
            tolerance_pct,
            tolerance_base,
        )
    ):
        return False
    if tolerance_pct < 0 or tolerance_base < 0:
        return False
    if expected_fee_base < 0 or actual_fee_base < 0:
        return False

    tol_abs = max(float(tolerance_base), abs(float(expected_fee_base)) * (float(tolerance_pct) / 100.0))
    return abs(float(expected_fee_base) - float(actual_fee_base)) <= tol_abs

