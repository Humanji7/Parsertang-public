"""
Network alias mappings for cryptocurrency networks.

This module provides canonical mappings between exchange-specific network names
and standardized network identifiers used throughout the system.
"""

from __future__ import annotations

# Canonical network alias mappings
# Maps exchange-specific names → standard names
NETWORK_ALIASES = {
    # Tron
    "TRX": "TRC20",
    "TRON": "TRC20",
    # Ethereum
    "ETH": "ERC20",
    "ETHEREUM": "ERC20",
    # Binance Smart Chain / Binance Chain
    "BSC": "BEP20",
    "BNB": "BEP20",
    "BEP20(BSC)": "BEP20",
    # Arbitrum
    "ARBONE": "ARB",
    "ARBITRUM": "ARB",
    "ARBITRUMONE": "ARB",
    "ARBNOVA": "ARBNOVA",  # Arbitrum Nova (separate network)
    # Optimism
    "OPTIMISM": "OPT",
    "OP": "OPT",
    # Polygon
    "MATIC": "POLYGON",
    "MATICPOLYGON": "POLYGON",
    # Avalanche C-Chain
    "AVAXC": "AVAX",
    "AVAXX": "AVAX",
    "AVAX-C": "AVAX",
    "AVAXCCHAIN": "AVAX",
    # MEXC-specific Avalanche variants (extracted from parentheses)
    "CCHAIN": "AVAX",  # From 'Avalanche C Chain(AVAX CCHAIN)'
    "XCHAIN": "AVAX",  # From 'Avalanche X Chain(AVAX XCHAIN)'
    # Base (Coinbase L2)
    "BASE": "BASE",
    # Ton
    "TON": "TON",
    "TONCOIN": "TON",
    # Solana
    "SOL": "SOL",
    "SOLANA": "SOL",
    # Bitcoin Cash
    "BCH": "BCHN",
    "BCHN": "BCHN",  # KuCoin uses bchn (lowercase becomes BCHN after .upper())
    "BITCOINCASH": "BCHN",
    # Bitcoin Lightning Network
    "BTCLN": "LIGHTNING",
    # Layer 2 networks
    "ZKSERA": "ZKSYNC",  # zkSync Era
    "MANTAETH": "MANTLE",  # Mantle
    "MANTLE": "MANTLE",  # Mantle (canonical)
    "PLASMA": "PLASMA",  # OMG Network
    "KAVAEVM": "KAVAEVM",  # Kava EVM
    # Cosmos ecosystem
    "ATOM1": "ATOM",  # HTX variant
    # Polkadot ecosystem
    "DOTAH": "DOT",  # Bybit: DOT Asset Hub
    "STATEMINT": "DOT",  # KuCoin: Statemint (old name for Asset Hub)
    "POLKADOT": "DOT",  # Generic alias
    "POLKADOTASSETHUB": "DOT",  # MEXC: Polkadot Asset Hub (no parentheses)
    "ASSETHUBPOLKADOT": "DOT",  # Alternative ordering
    # Story Network
    "STORY": "STORY",
    "IP": "STORY",  # Story IP chain
    # HyperEVM
    "HYPEREVM": "HYPEREVM",
    "HYPE": "HYPEREVM",
    # Plasma (XPL variant)
    "XPL": "PLASMA",
    # Other networks (canonical form)
    "SUI": "SUI",  # Sui
    "SONIC": "SONIC",  # Sonic
    "APTOS": "APTOS",  # Aptos
    "APT": "APTOS",  # Aptos (alternative)
    "APTOS_FA": "APTOS",  # OKX: Aptos fungible-asset network label
    "APTOS-FA": "APTOS",
}


def normalize_network(network: str | None) -> str | None:
    """
    Normalize network name using canonical aliases.

    Args:
        network: Raw network name from exchange

    Returns:
        Normalized network name or None if input is None

    Examples:
        >>> normalize_network("ARBONE")
        "ARB"
        >>> normalize_network("TRX")
        "TRC20"
        >>> normalize_network("Tron(TRC20)")
        "TRC20"
        >>> normalize_network("Solana(SOL)")
        "SOL"
        >>> normalize_network(None)
        None
    """
    if network is None:
        return None

    normalized = network.upper().strip()

    # Handle exchange-specific format: "Name(CODE)" → extract CODE from parentheses
    # Handles both simple and multi-word formats:
    # - "Tron(TRC20)" → "TRC20"
    # - "Avalanche C Chain(AVAX CCHAIN)" → "CCHAIN" → "AVAX" (via alias)
    import re

    paren_match = re.search(r"\(([^)]+)\)$", normalized)
    if paren_match:
        inner = paren_match.group(1).strip()
        # For multi-word codes like "AVAX CCHAIN", take the last word
        parts = inner.split()
        normalized = parts[-1] if parts else inner

    # Some exchanges suffix network labels with "_FA" / "-FA" (e.g., "Aptos_FA").
    # Treat these as the base network for cross-exchange matching.
    if normalized.endswith("_FA"):
        normalized = normalized[: -len("_FA")]
    elif normalized.endswith("-FA"):
        normalized = normalized[: -len("-FA")]

    return NETWORK_ALIASES.get(normalized, normalized)
