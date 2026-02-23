"""Tests for network alias normalization."""

from parsertang.network_aliases import normalize_network, NETWORK_ALIASES


def test_normalize_network_tron():
    assert normalize_network("TRX") == "TRC20"
    assert normalize_network("TRON") == "TRC20"


def test_normalize_network_arbitrum():
    assert normalize_network("ARBONE") == "ARB"
    assert normalize_network("ARBITRUM") == "ARB"


def test_normalize_network_optimism():
    assert normalize_network("OPTIMISM") == "OPT"


def test_normalize_network_polygon():
    assert normalize_network("MATIC") == "POLYGON"


def test_normalize_network_none():
    assert normalize_network(None) is None


def test_normalize_network_unknown():
    """Unknown networks should pass through unchanged."""
    assert normalize_network("UNKNOWN_NETWORK") == "UNKNOWN_NETWORK"


def test_normalize_network_case_insensitive():
    assert normalize_network("arbone") == "ARB"
    assert normalize_network("ArBOne") == "ARB"


def test_all_aliases_uppercase():
    """All alias keys should be uppercase for consistency."""
    for key in NETWORK_ALIASES.keys():
        assert key.isupper(), f"Alias key {key} is not uppercase"


def test_normalize_network_aptos():
    """Test Aptos network alias normalization."""
    assert normalize_network("APT") == "APTOS"
    assert normalize_network("apt") == "APTOS"
    assert normalize_network("APTOS") == "APTOS"


def test_normalize_network_mexc_multiword():
    """Test MEXC-style multi-word network names with spaces."""
    # MEXC returns formats like 'Avalanche C Chain(AVAX CCHAIN)'
    assert normalize_network("Avalanche C Chain(AVAX CCHAIN)") == "AVAX"
    assert normalize_network("Avalanche X Chain(AVAX XCHAIN)") == "AVAX"
    assert normalize_network("Bitcoin Cash(BCH)") == "BCHN"
    assert normalize_network("APTOS(APT)") == "APTOS"


def test_normalize_network_mexc_no_parentheses():
    """Test MEXC DOT format without parentheses."""
    assert normalize_network("POLKADOTASSETHUB") == "DOT"


def test_normalize_network_parentheses_format():
    """Test standard parentheses format still works."""
    assert normalize_network("Tron(TRC20)") == "TRC20"
    assert normalize_network("Solana(SOL)") == "SOL"
