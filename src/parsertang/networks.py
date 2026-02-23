from __future__ import annotations

import logging
from typing import Dict, Optional, Iterable

logger = logging.getLogger(__name__)


# Token-to-Network mapping: resolves the correct network when exchange APIs disagree
# This mapping helps handle cases where exchanges use different network names for the same token
# Example: Bybit may report "LINK" while KuCoin reports "ERC20" for LINK token
TOKEN_NETWORKS: Dict[str, str] = {
    # ERC-20 tokens → use ERC20 network
    "LINK": "ERC20",  # Chainlink
    "AAVE": "ERC20",  # Aave
    "CRV": "ERC20",  # Curve DAO
    "UNI": "ERC20",  # Uniswap
    "MKR": "ERC20",  # Maker
    "SNX": "ERC20",  # Synthetix
    "COMP": "ERC20",  # Compound
    "SHIB": "ERC20",  # Shiba Inu
    "1INCH": "ERC20",  # 1inch
    "BAT": "ERC20",  # Basic Attention Token
    "ENJ": "ERC20",  # Enjin Coin
    "MANA": "ERC20",  # Decentraland
    "ZRX": "ERC20",  # 0x
    # BEP-20 tokens → use BEP20 (Binance Smart Chain)
    "CAKE": "BEP20",  # PancakeSwap
    "BAKE": "BEP20",  # BakeryToken
    "BNB": "BEP20",  # Binance Coin (can use BEP2 or BEP20, BEP20 preferred)
    # SPL tokens → use SOL (Solana)
    "BONK": "SOL",  # Bonk
    "RAY": "SOL",  # Raydium
    "SRM": "SOL",  # Serum
    "COPE": "SOL",  # Cope
    "FIDA": "SOL",  # Bonfida
    "ORCA": "SOL",  # Orca
    "MNGO": "SOL",  # Mango
    "STEP": "SOL",  # Step Finance
    # Native blockchain coins (coin code = network code)
    "AVAX": "AVAX",  # Avalanche
    "NEAR": "NEAR",  # NEAR Protocol
    "ALGO": "ALGO",  # Algorand
    "ICP": "ICP",  # Internet Computer
    "FIL": "FIL",  # Filecoin
    "ETC": "ETC",  # Ethereum Classic
    "HBAR": "HBAR",  # Hedera
    "TRX": "TRC20",  # Tron native
    "XDC": "XDC",  # XinFin
    "DOT": "DOT",  # Polkadot
    "LTC": "LTC",  # Litecoin
    "XRP": "XRP",  # Ripple
    "DOGE": "DOGE",  # Dogecoin
    "BCH": "BCH",  # Bitcoin Cash
    "XLM": "XLM",  # Stellar
    "ADA": "ADA",  # Cardano
    "ATOM": "ATOM",  # Cosmos
    "APE": "APE",  # ApeCoin (native or could be ERC20)
    "APT": "APTOS",  # Aptos
    "SUI": "SUI",  # Sui
}


def resolve_network_for_token(
    base_currency: str,
    common_networks: set[str],
) -> Optional[str]:
    """
    Resolve network for a token when exchange APIs disagree.

    This function handles cases where different exchanges report different network names
    for the same token. For example:
    - Bybit reports "LINK" while KuCoin reports "ERC20" for LINK token
    - Bybit reports "BNB" while KuCoin reports "BEP20" for BNB token
    - Bybit reports "CRV" (token symbol) while KuCoin reports "ERC20" (network)

    Args:
        base_currency: Token symbol (e.g., 'CAKE', 'LINK', 'BNB')
        common_networks: Networks found in common between exchanges (can be empty)

    Returns:
        Network name or None if unable to resolve
    """
    # Get the token's preferred network from mapping
    preferred = TOKEN_NETWORKS.get(base_currency)

    # If there are common networks
    if common_networks:
        # If preferred network is in common networks, use it
        if preferred and preferred in common_networks:
            return preferred

        # Filter out networks that match the token symbol itself
        # (Bybit sometimes incorrectly reports token symbol as network)
        valid_networks = {net for net in common_networks if net != base_currency}

        # If we have valid networks after filtering, use the first one
        if valid_networks:
            return next(iter(valid_networks))

        # If preferred network exists but wasn't in common, try using it anyway
        # This handles cases where exchanges disagree completely
        if preferred:
            return preferred

    # Fallback: if no common networks or all filtered out, use token mapping
    return preferred


def pick_best_network(
    common_networks: Iterable[str],
    per_exchange_fees: Dict[str, float] | None,
    trade_volume_usd: float,
) -> tuple[Optional[str], Optional[str]]:
    """
    Pick the best network based on withdrawal fees.

    Selects the cheapest network from available options using dynamic fees
    fetched from exchanges. Networks without fee data are skipped.

    Args:
        common_networks: Networks available on both exchanges
        per_exchange_fees: Dict mapping network codes to withdrawal fees in USD
        trade_volume_usd: Trade volume (unused, kept for API compatibility)

    Returns:
        Tuple of (network, error_reason):
        - (network_name, None) if network selected successfully
        - (None, "no_fee_data") if per_exchange_fees is None or empty
        - (None, "no_valid_networks") if all networks filtered out
    """
    if not per_exchange_fees:
        # No fee data available, cannot determine best network
        # DEBUG level: the root cause is logged in withdrawal_fees.py (FEE LOOKUP MISMATCH)
        logger.debug("NETWORK SELECT | No fee data available")
        return None, "no_fee_data"

    candidates = []
    for net in common_networks:
        # Only consider networks with known fees
        if net in per_exchange_fees:
            fee = per_exchange_fees[net]
            candidates.append((fee, net))

    if not candidates:
        logger.warning(
            "NETWORK SELECT | No valid networks found (common=%s, fees=%s)",
            list(common_networks),
            list(per_exchange_fees.keys()),
        )
        return None, "no_valid_networks"

    # Sort by fee (ascending) and return the cheapest
    candidates.sort()
    best_network = candidates[0][1]
    logger.debug(
        "NETWORK SELECT | Best network=%s fee=%.4f", best_network, candidates[0][0]
    )
    return best_network, None
