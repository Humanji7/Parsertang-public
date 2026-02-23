"""
Exchange credential configuration builder for authenticated API access.

This module provides utilities to build ccxt.pro exchange configurations
with API credentials from Settings, enabling authenticated requests for
withdrawal fee fetching and other private endpoints.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from parsertang.config import Settings


def build_exchange_config(
    exchange_id: str,
    settings: Settings,
    proxy_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Build ccxt.pro exchange config with API credentials.

    Creates a configuration dictionary for initializing ccxt.pro Exchange instances
    with authentication. Handles exchange-specific requirements (e.g., OKX and KuCoin
    require password/passphrase fields).

    Args:
        exchange_id: Exchange identifier (e.g., 'bybit', 'okx', 'kucoin')
        settings: Application settings containing API credentials
        proxy_config: Optional proxy configuration dict with http/https/socks keys

    Returns:
        Dict containing exchange configuration:
        - enableRateLimit: Always True
        - proxies: Optional proxy settings
        - apiKey: API key if configured
        - secret: API secret if configured
        - password: Passphrase for exchanges that require it (OKX, KuCoin)

    Example:
        >>> config = build_exchange_config('bybit', settings, None)
        >>> exchange = ccxt.pro.bybit(config)
    """
    config: dict[str, Any] = {"enableRateLimit": True}

    # Add proxy if configured
    if proxy_config and any(proxy_config.values()):
        config["proxies"] = proxy_config

    # Add credentials per exchange
    if exchange_id == "bybit":
        if settings.bybit_api_key:
            config["apiKey"] = settings.bybit_api_key
            config["secret"] = settings.bybit_secret

    elif exchange_id == "okx":
        if settings.okx_api_key:
            config["apiKey"] = settings.okx_api_key
            config["secret"] = settings.okx_secret
            config["password"] = settings.okx_password  # OKX requires passphrase

    elif exchange_id == "kucoin":
        if settings.kucoin_api_key:
            config["apiKey"] = settings.kucoin_api_key
            config["secret"] = settings.kucoin_secret
            config["password"] = settings.kucoin_password  # KuCoin requires passphrase

    elif exchange_id == "htx":
        if settings.htx_api_key:
            config["apiKey"] = settings.htx_api_key
            config["secret"] = settings.htx_secret
        # HTX: Use SOCKS5 proxy to bypass Singapore geo-block for BOTH REST and WebSocket
        # Singapore blocked since March 2022 (MAS regulation)
        # NOTE: HTTP proxy only works for REST API, WebSocket requires SOCKS5
        raw_htx_proxy = settings.htx_proxy
        proxy_disabled = isinstance(
            raw_htx_proxy, str
        ) and raw_htx_proxy.strip().lower() in {"off", "none", "false", "0"}
        if not proxy_disabled:
            htx_proxy = (
                raw_htx_proxy or "socks5://heyxyvmx:w08kytmbsbid@31.98.13.241:6418"
            )
            # IMPORTANT: Remove any existing proxies dict to avoid conflict with socksProxy
            # CCXT throws "conflicting proxy settings" if both are present
            config.pop("proxies", None)
            # SOCKS5 for REST API
            config["socksProxy"] = htx_proxy
            # SOCKS5 for WebSocket connections (critical for geo-block bypass)
            config["wsSocksProxy"] = htx_proxy
        # HTX: Use AWS-optimized endpoint to bypass Singapore geo-restrictions
        # Standard api.huobi.pro blocked for Singapore IPs since March 2022 (MAS regulation)
        config["hostname"] = "api-aws.huobi.pro"
        # HTX: Disable derivatives markets (linear/inverse) that use api.hbdm.com
        # which is geo-blocked in many regions. Only load spot markets.
        config["options"] = {
            "defaultType": "spot",
            "fetchMarkets": {
                "types": {
                    "spot": True,
                    "linear": False,  # Skip linear swaps (api.hbdm.com)
                    "inverse": False,  # Skip inverse futures (api.hbdm.com)
                }
            },
        }

    elif exchange_id == "gate":
        if settings.gate_api_key:
            config["apiKey"] = settings.gate_api_key
            config["secret"] = settings.gate_secret

    elif exchange_id == "mexc":
        if settings.mexc_api_key:
            config["apiKey"] = settings.mexc_api_key
            config["secret"] = settings.mexc_secret
            # MEXC can intermittently reject authenticated requests with:
            #   {"code":700003,"msg":"Timestamp for this request is outside of the recvWindow."}
            # Even when the host clock is NTP-synced, a small server-side offset or
            # transient delays can exceed the tight default recvWindow (5s).
            # Make the behavior more resilient and keep markets loading stable.
            config["options"] = {
                "adjustForTimeDifference": True,
                "recvWindow": 20_000,
            }

    return config
