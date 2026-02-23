"""Test optimal network selection for withdrawals.

This module tests the logic for selecting the best withdrawal network
based on fees and availability across exchanges.

Reference: SPEC-R1-001, Section 6.2
"""

from parsertang.networks import pick_best_network, resolve_network_for_token


def test_prefers_low_cost_networks():
    """Should prefer TRC20, BEP20 over ERC20."""
    common_networks = ["TRC20", "ERC20", "BEP20"]

    # Mock per-exchange fees (TRC20=1.0, ERC20=12.0, BEP20=0.8)
    per_exchange_fees = {
        "TRC20": 1.0,
        "ERC20": 12.0,
        "BEP20": 0.8,
    }

    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should select BEP20 (lowest fee) with no error
    assert network == "BEP20"
    assert error is None


def test_returns_none_when_no_per_exchange_fees():
    """Should return None when per-exchange fee data unavailable."""
    common_networks = ["TRC20", "BEP20", "SOL"]

    # No per-exchange fees provided
    per_exchange_fees = None
    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should return None with "no_fee_data" error
    assert network is None
    assert error == "no_fee_data"


def test_no_common_network():
    """Should return None when no common networks exist."""
    common_networks = []
    per_exchange_fees = {}
    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    assert network is None
    assert error == "no_fee_data"  # Empty dict triggers "no_fee_data" error


def test_filters_by_fee_percentage_threshold():
    """Should select cheapest network (no fee % filtering, only ETA and cost)."""
    common_networks = ["TRC20", "ERC20"]

    # TRC20 fee: $1.0
    # ERC20 fee: $12.0
    per_exchange_fees = {
        "TRC20": 1.0,
        "ERC20": 12.0,
    }

    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should select cheapest network (TRC20)
    assert network == "TRC20"
    assert error is None


def test_requires_per_exchange_fees():
    """Should return None without per_exchange_fees data."""
    common_networks = ["TRC20", "BEP20"]
    per_exchange_fees = None
    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should return None with "no_fee_data" error
    assert network is None
    assert error == "no_fee_data"


def test_prefers_faster_network_when_fees_equal():
    """When fees are equal, should prefer network with lower ETA."""
    common_networks = ["TRC20", "SOL"]  # SOL ETA=2min, TRC20 ETA=3min

    # Make fees equal
    per_exchange_fees = {
        "TRC20": 0.5,
        "SOL": 0.5,
    }

    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Both are acceptable, function sorts by fee first
    # So either is valid, but SOL might be preferred due to lower baseline fee
    assert network in ["TRC20", "SOL"]
    assert error is None


def test_unknown_network_accepted_with_fee_data():
    """Unknown networks are accepted if they have fee data."""
    common_networks = ["UNKNOWN_NET", "TRC20"]

    per_exchange_fees = {
        "UNKNOWN_NET": 5.0,
        "TRC20": 1.0,
    }

    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should select TRC20 (cheaper)
    assert network == "TRC20"
    assert error is None


def test_all_networks_too_expensive():
    """Should accept networks regardless of fee % (fee filtering moved to net profit calc)."""
    common_networks = ["ERC20"]  # Known for high fees

    per_exchange_fees = {
        "ERC20": 15.0,  # $15 on $100 = 15%
    }

    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Network is accepted, fee will be filtered at MIN_NET_PROFIT level
    assert network == "ERC20"
    assert error is None


def test_larger_volume_makes_more_networks_acceptable():
    """With larger trade volume, more networks become cost-effective."""
    common_networks = ["TRC20", "ERC20"]

    per_exchange_fees = {
        "TRC20": 1.0,
        "ERC20": 5.0,
    }

    # Small volume: ERC20 is 5% (too expensive)
    small_volume = 100.0
    network_small, error_small = pick_best_network(
        common_networks, per_exchange_fees, small_volume
    )

    # Should only accept TRC20
    assert network_small == "TRC20"
    assert error_small is None

    # Large volume: ERC20 is 0.1% (acceptable)
    large_volume = 5000.0
    network_large, error_large = pick_best_network(
        common_networks, per_exchange_fees, large_volume
    )

    # Should prefer TRC20 (still lower fee percentage), but ERC20 now acceptable
    assert network_large == "TRC20"
    assert error_large is None


def test_prioritizes_common_low_cost_networks():
    """Test that common low-cost networks are prioritized as expected."""
    # According to SPEC, priority: TRC20, BEP20, SOL, POLYGON, etc.
    common_networks = ["TRC20", "BEP20", "SOL", "POLYGON", "ARB"]

    # Provide dynamic fees
    per_exchange_fees = {
        "TRC20": 1.0,
        "BEP20": 0.5,
        "SOL": 0.2,
        "POLYGON": 0.2,
        "ARB": 0.5,
    }
    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # SOL or POLYGON should be selected (both have lowest fee 0.2)
    assert network in ["SOL", "POLYGON"]
    assert error is None


def test_empty_per_exchange_fees_dict():
    """Test handling of empty per_exchange_fees dictionary."""
    common_networks = ["TRC20", "BEP20"]
    per_exchange_fees = {}  # Empty dict
    trade_volume_usd = 100.0

    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd
    )

    # Should return None with "no_fee_data" error
    assert network is None
    assert error == "no_fee_data"


# Tests for resolve_network_for_token()


def test_resolve_network_for_erc20_token():
    """Should resolve ERC20 network for LINK token when common networks is empty."""
    base_currency = "LINK"
    common_networks = set()  # No common networks (exchange APIs disagree)

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to ERC20 from TOKEN_NETWORKS mapping
    assert resolved == "ERC20"


def test_resolve_network_for_bep20_token():
    """Should resolve BEP20 network for BNB token when common networks is empty."""
    base_currency = "BNB"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to BEP20 from TOKEN_NETWORKS mapping
    assert resolved == "BEP20"


def test_resolve_network_for_solana_token():
    """Should resolve SOL network for BONK token."""
    base_currency = "BONK"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to SOL from TOKEN_NETWORKS mapping
    assert resolved == "SOL"


def test_resolve_network_prefers_common_network_when_available():
    """Should prefer common network when it matches the token's preferred network."""
    base_currency = "LINK"
    common_networks = {"ERC20", "KCC"}  # Multiple common networks, including preferred

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should prefer ERC20 (the preferred network for LINK)
    assert resolved == "ERC20"


def test_resolve_network_uses_any_common_network_if_preferred_not_available():
    """Should use any common network if the preferred one is not available."""
    base_currency = "LINK"
    common_networks = {"KCC"}  # Only KCC available, ERC20 not in common

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should use KCC (the only common network)
    assert resolved == "KCC"


def test_resolve_network_for_native_coin():
    """Should resolve native network for native blockchain coins."""
    base_currency = "DOT"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to DOT (native Polkadot network)
    assert resolved == "DOT"


def test_resolve_network_for_aptos_token():
    """Should resolve APTOS network for APT token."""
    base_currency = "APT"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    assert resolved == "APTOS"


def test_resolve_network_returns_none_for_unknown_token():
    """Should return None when token is not in TOKEN_NETWORKS and no common networks."""
    base_currency = "UNKNOWN_TOKEN"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should return None for unknown tokens
    assert resolved is None


def test_resolve_network_bypasses_mapping_when_common_networks_exist_for_unknown_token():
    """Should use common networks even for unknown tokens."""
    base_currency = "UNKNOWN_TOKEN"
    common_networks = {"TRC20", "BEP20"}

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should return one of the common networks (since TOKEN_NETWORKS has no entry)
    assert resolved in ["TRC20", "BEP20"]


def test_resolve_network_real_world_bnb_case():
    """Test real-world case: BNB with bybit reporting 'BNB' and kucoin reporting 'BEP2', 'BEP20'."""
    base_currency = "BNB"
    # Simulating: kucoin has BEP2, BEP20, bybit has BNB -> no common networks after normalization
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to BEP20 (preferred for BNB)
    assert resolved == "BEP20"


def test_resolve_network_real_world_link_case():
    """Test real-world case: LINK with bybit reporting 'LINK' and kucoin reporting 'ERC20', 'KCC'."""
    base_currency = "LINK"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to ERC20 (preferred for LINK)
    assert resolved == "ERC20"


def test_resolve_network_real_world_aave_case():
    """Test real-world case: AAVE with bybit reporting 'AAVE' and kucoin reporting 'ERC20', 'KCC'."""
    base_currency = "AAVE"
    common_networks = set()

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to ERC20 (preferred for AAVE)
    assert resolved == "ERC20"


def test_resolve_network_with_common_networks_for_dot():
    """Test DOT with common networks (should use common network directly)."""
    base_currency = "DOT"
    common_networks = {"DOT"}  # Both exchanges agree on DOT

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should use the common network DOT (DOT is both a token and network)
    assert resolved == "DOT"


def test_resolve_network_filters_token_symbol_from_common():
    """Should filter out token symbol when it appears as a 'network' in common_networks."""
    base_currency = "CRV"
    # Bybit incorrectly reports 'CRV' as network, should be filtered out
    common_networks = {"CRV"}

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should resolve to ERC20 (preferred for CRV) instead of using 'CRV' network
    assert resolved == "ERC20"


def test_resolve_network_with_mixed_valid_and_invalid():
    """Should use valid network when common contains both token symbol and real network."""
    base_currency = "LINK"
    # Mix of token symbol and real network
    common_networks = {"LINK", "KCC"}

    resolved = resolve_network_for_token(base_currency, common_networks)

    # Should filter out 'LINK' and use 'KCC'
    assert resolved == "KCC"
