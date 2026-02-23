"""
Tests for dynamic withdrawal fee fetching and caching (SPEC-FEE-001 Phase 1).

Coverage targets:
- WithdrawalFeeCache: get_fee(), is_stale()
- WithdrawalFeeManager: fetch_all_fees(), refresh_cache(), get_withdrawal_fee()
- Network normalization: _normalize_network_code()
- Background refresh: timing, error handling, graceful shutdown
- Fallback logic: cache miss, API failures
"""

import asyncio
import time
from unittest.mock import AsyncMock, Mock

import pytest

from parsertang.withdrawal_fees import WithdrawalFeeCache, WithdrawalFeeManager


class TestWithdrawalFeeCache:
    """Tests for WithdrawalFeeCache dataclass."""

    def test_cache_stores_and_retrieves_fees(self):
        """Test basic fee storage and retrieval."""
        fees = {
            "bybit": {
                "USDT": {"TRC20": 0.8, "ERC20": 15.0},
                "USDC": {"TRC20": 0.8},
            },
            "okx": {
                "USDT": {"TRC20": 1.0, "BEP20": 0.5},
            },
        }

        cache = WithdrawalFeeCache(
            fees=fees,
            last_updated=time.time(),
            cache_lifetime=3600.0,
        )

        # Test successful retrieval
        assert cache.get_fee("bybit", "USDT", "TRC20") == 0.8
        assert cache.get_fee("bybit", "USDT", "ERC20") == 15.0
        assert cache.get_fee("okx", "USDT", "TRC20") == 1.0
        assert cache.get_fee("okx", "USDT", "BEP20") == 0.5

    def test_cache_returns_none_for_missing_fees(self):
        """Test that missing fees return None."""
        fees = {
            "bybit": {
                "USDT": {"TRC20": 0.8},
            },
        }

        cache = WithdrawalFeeCache(
            fees=fees,
            last_updated=time.time(),
            cache_lifetime=3600.0,
        )

        # Test missing entries at each level
        assert cache.get_fee("unknown_exchange", "USDT", "TRC20") is None
        assert cache.get_fee("bybit", "BTC", "TRC20") is None
        assert cache.get_fee("bybit", "USDT", "BEP20") is None

    def test_cache_is_not_stale_when_fresh(self):
        """Test cache is not stale immediately after creation."""
        cache = WithdrawalFeeCache(
            fees={},
            last_updated=time.time(),
            cache_lifetime=3600.0,
        )

        assert not cache.is_stale()

    def test_cache_is_stale_after_lifetime(self):
        """Test cache becomes stale after lifetime expires."""
        cache = WithdrawalFeeCache(
            fees={},
            last_updated=time.time() - 3700,  # 3700 seconds ago
            cache_lifetime=3600.0,  # 1 hour
        )

        assert cache.is_stale()

    def test_cache_boundary_at_exact_lifetime(self):
        """Test cache staleness at exact lifetime boundary."""
        now = time.time()
        cache = WithdrawalFeeCache(
            fees={},
            last_updated=now - 3600,  # Exactly 1 hour ago
            cache_lifetime=3600.0,
        )

        # At exactly the lifetime, should be stale
        assert cache.is_stale()


class TestWithdrawalFeeManagerNetworkNormalization:
    """Tests for network code normalization."""

    def test_normalize_removes_trailing_currency_suffix(self):
        """Test normalization of "TRC20-USDT" → "TRC20"."""
        result = WithdrawalFeeManager._normalize_network_code("TRC20-USDT", "USDT")
        assert result == "TRC20"

        result = WithdrawalFeeManager._normalize_network_code("ERC20-USDC", "USDC")
        assert result == "ERC20"

    def test_normalize_removes_leading_currency_prefix(self):
        """Test normalization of "USDT-TRC20" → "TRC20"."""
        result = WithdrawalFeeManager._normalize_network_code("USDT-TRC20", "USDT")
        assert result == "TRC20"

        result = WithdrawalFeeManager._normalize_network_code("USDC-BEP20", "USDC")
        assert result == "BEP20"

    def test_normalize_handles_exchange_specific_aliases(self):
        """Test normalization of exchange-specific network names."""
        # Tron aliases
        assert WithdrawalFeeManager._normalize_network_code("TRX", "") == "TRC20"

        # Ethereum aliases
        assert WithdrawalFeeManager._normalize_network_code("ETH", "") == "ERC20"

        # Binance Smart Chain aliases
        assert WithdrawalFeeManager._normalize_network_code("BSC", "") == "BEP20"
        assert WithdrawalFeeManager._normalize_network_code("BNB", "") == "BEP20"

        # Polygon aliases
        assert WithdrawalFeeManager._normalize_network_code("MATIC", "") == "POLYGON"

        # Avalanche aliases
        assert WithdrawalFeeManager._normalize_network_code("AVAXC", "") == "AVAX"

    def test_normalize_preserves_standard_network_codes(self):
        """Test that standard codes remain unchanged."""
        assert WithdrawalFeeManager._normalize_network_code("TRC20", "") == "TRC20"
        assert WithdrawalFeeManager._normalize_network_code("ERC20", "") == "ERC20"
        assert WithdrawalFeeManager._normalize_network_code("BEP20", "") == "BEP20"
        assert WithdrawalFeeManager._normalize_network_code("SOL", "") == "SOL"
        assert WithdrawalFeeManager._normalize_network_code("ARB", "") == "ARB"

    def test_normalize_handles_whitespace(self):
        """Test that whitespace is properly stripped."""
        assert WithdrawalFeeManager._normalize_network_code(" TRC20 ", "") == "TRC20"
        assert (
            WithdrawalFeeManager._normalize_network_code("  ERC20-USDT  ", "USDT")
            == "ERC20"
        )

    def test_normalize_is_case_insensitive(self):
        """Test that normalization handles lowercase input."""
        assert WithdrawalFeeManager._normalize_network_code("trc20", "") == "TRC20"
        assert WithdrawalFeeManager._normalize_network_code("erc20", "") == "ERC20"
        assert WithdrawalFeeManager._normalize_network_code("bsc", "") == "BEP20"

    def test_normalize_arbitrum_variants(self):
        """Test normalization of Arbitrum network variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("ARBONE", "USDT") == "ARB"
        assert manager._normalize_network_code("ARBITRUM", "USDT") == "ARB"
        assert manager._normalize_network_code("ARBNOVA", "USDT") == "ARBNOVA"

    def test_normalize_optimism_variants(self):
        """Test normalization of Optimism network variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("OPTIMISM", "USDT") == "OPT"
        assert manager._normalize_network_code("OP", "USDT") == "OPT"

    def test_normalize_avalanche_variants(self):
        """Test normalization of Avalanche network variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("AVAXC", "USDT") == "AVAX"
        assert manager._normalize_network_code("AVAXCCHAIN", "USDT") == "AVAX"

    def test_normalize_case_variants(self):
        """Test normalization handles case variations."""
        manager = WithdrawalFeeManager(exchanges={})

        # Lowercase variants should normalize to uppercase
        assert manager._normalize_network_code("sui", "USDC") == "SUI"
        assert manager._normalize_network_code("SUI", "USDC") == "SUI"
        assert manager._normalize_network_code("sonic", "USDC") == "SONIC"
        assert manager._normalize_network_code("plasma", "USDT") == "PLASMA"
        assert manager._normalize_network_code("kavaevm", "USDT") == "KAVAEVM"

    def test_normalize_layer2_variants(self):
        """Test normalization of Layer 2 network variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("ZKSERA", "USDC") == "ZKSYNC"
        assert manager._normalize_network_code("MANTAETH", "ETH") == "MANTLE"

    def test_normalize_bitcoin_variants(self):
        """Test normalization of Bitcoin network variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("btcln", "BTC") == "LIGHTNING"
        assert manager._normalize_network_code("BTCLN", "BTC") == "LIGHTNING"

    def test_normalize_cosmos_variants(self):
        """Test normalization of Cosmos ecosystem variants."""
        manager = WithdrawalFeeManager(exchanges={})

        assert manager._normalize_network_code("ATOM1", "ATOM") == "ATOM"
        assert manager._normalize_network_code("ATOM", "ATOM") == "ATOM"

    def test_normalize_combined_suffix_and_alias(self):
        """Test normalization with both currency suffix removal and alias mapping."""
        manager = WithdrawalFeeManager(exchanges={})

        # Test suffix removal followed by alias mapping
        # "USDT-ARBONE" → "ARBONE" → "ARB"
        assert manager._normalize_network_code("USDT-ARBONE", "USDT") == "ARB"

        # "ARBONE-USDT" → "ARBONE" → "ARB"
        assert manager._normalize_network_code("ARBONE-USDT", "USDT") == "ARB"

        # "USDT-OPTIMISM" → "OPTIMISM" → "OPT"
        assert manager._normalize_network_code("USDT-OPTIMISM", "USDT") == "OPT"


@pytest.mark.asyncio
class TestWithdrawalFeeManagerFetching:
    """Tests for fee fetching from exchange APIs."""

    async def test_fetch_exchange_fees_success(self):
        """Test successful fee fetching from single exchange."""
        # Mock exchange with ccxt.pro response format
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                        "ERC20": {"withdraw": {"fee": 15.0}},
                    },
                },
                "USDC": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,  # Boolean, not dict
                            "active": True,  # At network level
                            "fee": 0.8,  # At network level
                        },
                        "ERC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 15.0,
                        },
                    },
                },
                "USDC": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 0.8,
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # Verify structure
        assert "USDT" in fees
        assert "USDC" in fees
        assert fees["USDT"]["TRC20"] == 0.8
        assert fees["USDT"]["ERC20"] == 15.0
        assert fees["USDC"]["TRC20"] == 0.8

        # Verify fetch was called
        mock_exchange.fetch_currencies.assert_called_once()

    async def test_fetch_exchange_fees_skips_inactive_networks(self):
        """Test that networks with active=false are skipped."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 0.8,
                        },
                        "ERC20": {
                            "withdraw": True,
                            "active": False,  # Inactive network
                            "fee": 15.0,
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # ERC20 should be skipped
        assert "TRC20" in fees["USDT"]
        assert "ERC20" not in fees["USDT"]

    async def test_fetch_exchange_fees_handles_zero_and_missing_fees(self):
        """Test that zero fees are saved (free withdrawal) but missing fees are skipped."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 0.8,
                        },
                        "BEP20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 0,  # Zero fee = FREE withdrawal, should save
                        },
                        "SOL": {
                            "withdraw": True,
                            "active": True,
                            # Missing fee (None) - should skip
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # TRC20 and BEP20 should be included (BEP20 has fee=0 = free)
        assert "TRC20" in fees["USDT"]
        assert "BEP20" in fees["USDT"]  # fee=0 is valid (free withdrawal)
        assert fees["USDT"]["BEP20"] == 0.0
        assert "SOL" not in fees["USDT"]  # Missing fee = skip

    async def test_fetch_exchange_fees_handles_timeout(self):
        """Test graceful handling of API timeout."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(side_effect=asyncio.TimeoutError())

        manager = WithdrawalFeeManager(
            exchanges={"bybit": mock_exchange},
            fetch_timeout=1.0,
        )

        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # Should return empty dict on error
        assert fees == {}

    async def test_fetch_exchange_fees_handles_api_error(self):
        """Test graceful handling of API exceptions."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(side_effect=Exception("API error"))

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # Should return empty dict on error
        assert fees == {}

    async def test_fetch_exchange_fees_normalizes_network_codes(self):
        """Test that network codes are normalized during fetch."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20-USDT": {  # OKX-style suffix
                            "withdraw": True,
                            "active": True,
                            "fee": 1.0,
                        },
                        "USDT-BEP20": {  # Alternative format
                            "withdraw": True,
                            "active": True,
                            "fee": 0.5,
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"okx": mock_exchange})
        fees = await manager._fetch_exchange_fees("okx", mock_exchange)

        # Networks should be normalized
        assert "TRC20" in fees["USDT"]
        assert "BEP20" in fees["USDT"]
        assert fees["USDT"]["TRC20"] == 1.0
        assert fees["USDT"]["BEP20"] == 0.5

    async def test_fetch_all_fees_combines_multiple_exchanges(self):
        """Test fetching from multiple exchanges simultaneously."""
        mock_bybit = Mock()
        mock_bybit.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_bybit.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                    },
                },
            }
        )

        mock_okx = Mock()
        mock_okx.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 1.0},
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_bybit, "okx": mock_okx})
        all_fees = await manager.fetch_all_fees()

        # Verify both exchanges present
        assert "bybit" in all_fees
        assert "okx" in all_fees

        # Verify per-exchange fees differ
        assert all_fees["bybit"]["USDT"]["TRC20"] == 0.8
        assert all_fees["okx"]["USDT"]["TRC20"] == 1.0

    async def test_fetch_all_fees_handles_partial_failures(self):
        """Test that partial failures don't prevent successful fetches."""
        mock_bybit = Mock()
        mock_bybit.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_bybit.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                    },
                },
            }
        )

        mock_okx = Mock()
        mock_okx.fetch_currencies = AsyncMock(side_effect=Exception("API error"))

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_bybit, "okx": mock_okx})
        all_fees = await manager.fetch_all_fees()

        # Bybit should have data, OKX should be empty
        assert len(all_fees["bybit"]["USDT"]) > 0
        assert all_fees["okx"] == {}


@pytest.mark.asyncio
class TestWithdrawalFeeManagerCaching:
    """Tests for cache management."""

    async def test_refresh_cache_updates_cache(self):
        """Test that refresh_cache updates the cache."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})

        # Initially no cache
        assert manager.cache is None

        # Refresh cache
        await manager.refresh_cache()

        # Cache should exist
        assert manager.cache is not None
        assert manager.cache.get_fee("bybit", "USDT", "TRC20") == 0.8

    async def test_refresh_cache_updates_timestamp(self):
        """Test that refresh updates the timestamp."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(return_value={})

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})

        before_time = time.time()
        await manager.refresh_cache()
        after_time = time.time()

        assert manager.cache.last_updated >= before_time
        assert manager.cache.last_updated <= after_time

    async def test_start_background_refresh_performs_initial_fetch(self):
        """Test that starting background refresh does initial fetch."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})

        await manager.start_background_refresh()

        # Cache should exist after initial fetch
        assert manager.cache is not None
        assert manager.cache.get_fee("bybit", "USDT", "TRC20") == 0.8

        # Cleanup
        await manager.stop_background_refresh()

    async def test_stop_background_refresh_cancels_task(self):
        """Test that stopping background refresh cancels the task."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(return_value={})

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})

        await manager.start_background_refresh()
        assert manager._refresh_task is not None

        await manager.stop_background_refresh()
        assert manager._refresh_task is None


@pytest.mark.asyncio
class TestWithdrawalFeeManagerLookup:
    """Tests for fee lookup with fallback."""

    async def test_get_withdrawal_fee_cache_hit(self):
        """Test successful cache hit."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        await manager.refresh_cache()

        # Should return cached value with HIGH confidence
        fee, confidence = manager.get_withdrawal_fee("bybit", "USDT", "TRC20")
        assert fee == 0.8
        assert confidence == "HIGH"

    async def test_get_withdrawal_fee_missing_returns_none(self):
        """Test that missing fees return None when not in cache."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(return_value={})

        manager = WithdrawalFeeManager(
            exchanges={"bybit": mock_exchange},
        )
        await manager.refresh_cache()

        # Should return (None, "HIGH") for missing fee (not 0.0, which means free withdrawal)
        fee, confidence = manager.get_withdrawal_fee("bybit", "BTC", "UNKNOWN_NETWORK")
        assert fee is None
        assert confidence == "HIGH"

    async def test_get_per_exchange_fees_returns_dict(self):
        """Test get_per_exchange_fees returns fee dict for multiple networks."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": {"fee": 0.8}},
                        "ERC20": {"withdraw": {"fee": 15.0}},
                        "BEP20": {"withdraw": {"fee": 0.5}},
                    },
                },
            }
        )
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {"withdraw": True, "active": True, "fee": 0.8},
                        "ERC20": {"withdraw": True, "active": True, "fee": 15.0},
                        "BEP20": {"withdraw": True, "active": True, "fee": 0.5},
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        await manager.refresh_cache()

        fees = manager.get_per_exchange_fees_usd(
            exchange_id="bybit",
            currency="USDT",
            networks=["TRC20", "ERC20", "BEP20", "SOL"],  # SOL not in cache
            current_price_usd=1.0,  # USDT = $1.00
        )

        # Should return fees for cached networks
        assert fees["TRC20"] == 0.8
        assert fees["ERC20"] == 15.0
        assert fees["BEP20"] == 0.5

        # SOL not in cache, returns 0.0, should be excluded from dict
        assert "SOL" not in fees

    async def test_get_per_exchange_fees_excludes_zero_fees(self):
        """Test that zero fees are excluded from per_exchange_fees."""
        manager = WithdrawalFeeManager(
            exchanges={},
        )

        # No cache, no data → all zeros
        fees = manager.get_per_exchange_fees_usd(
            exchange_id="bybit",
            currency="BTC",
            networks=["UNKNOWN"],
            current_price_usd=50000.0,  # BTC = $50,000
        )

        # Zero fees should be excluded
        assert "UNKNOWN" not in fees


@pytest.mark.asyncio
class TestGateFallback:
    """Tests for Gate.io fallback using fetch_deposit_withdraw_fees()."""

    async def test_fetch_fees_fallback_success(self):
        """Test successful Gate.io fallback fee fetching."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": {"fee": 1.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                        "ERC20": {
                            "withdraw": {"fee": 10.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
                "XRP": {
                    "networks": {
                        "XRP": {
                            "withdraw": {"fee": 0.1, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # Verify structure
        assert "USDT" in fees
        assert "XRP" in fees
        assert fees["USDT"]["TRC20"] == 1.0
        assert fees["USDT"]["ERC20"] == 10.0
        assert fees["XRP"]["XRP"] == 0.1

        # Verify fetch was called
        mock_exchange.fetch_deposit_withdraw_fees.assert_called_once()

    async def test_fetch_fees_fallback_skips_invalid_withdraw_info(self):
        """Test that networks with non-dict withdraw info are skipped."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": {"fee": 1.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                        "ERC20": {
                            "withdraw": "invalid",  # Not a dict
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # ERC20 should be skipped
        assert "TRC20" in fees["USDT"]
        assert "ERC20" not in fees["USDT"]

    async def test_fetch_fees_fallback_handles_zero_and_missing_fees(self):
        """Test that zero fees are saved (free withdrawal) but missing fees are skipped."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": {"fee": 1.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                        "BEP20": {
                            "withdraw": {"fee": 0, "percentage": False},  # Zero = FREE
                            "deposit": {"fee": None, "percentage": None},
                        },
                        "SOL": {
                            "withdraw": {"percentage": False},  # Missing fee (None)
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # TRC20 and BEP20 should be included (BEP20 has fee=0 = free)
        assert "TRC20" in fees["USDT"]
        assert "BEP20" in fees["USDT"]  # fee=0 is valid (free withdrawal)
        assert fees["USDT"]["BEP20"] == 0.0
        assert "SOL" not in fees["USDT"]  # Missing fee = skip

    async def test_fetch_fees_fallback_handles_timeout(self):
        """Test graceful handling of API timeout."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            side_effect=asyncio.TimeoutError()
        )

        manager = WithdrawalFeeManager(
            exchanges={"gate": mock_exchange},
            fetch_timeout=1.0,
        )

        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # Should return empty dict on error
        assert fees == {}

    async def test_fetch_fees_fallback_handles_api_error(self):
        """Test graceful handling of API exceptions."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            side_effect=Exception("API error")
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # Should return empty dict on error
        assert fees == {}

    async def test_fetch_fees_fallback_normalizes_network_codes(self):
        """Test that network codes are normalized during fallback fetch."""
        mock_exchange = Mock()
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20-USDT": {  # With suffix
                            "withdraw": {"fee": 1.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                        "USDT-BEP20": {  # Alternative format
                            "withdraw": {"fee": 0.5, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_fees_fallback("gate", mock_exchange)

        # Networks should be normalized
        assert "TRC20" in fees["USDT"]
        assert "BEP20" in fees["USDT"]
        assert fees["USDT"]["TRC20"] == 1.0
        assert fees["USDT"]["BEP20"] == 0.5

    async def test_fetch_exchange_fees_triggers_gate_fallback(self):
        """Test that _fetch_exchange_fees triggers fallback for Gate when no networks found."""
        mock_exchange = Mock()

        # First call (fetch_currencies) returns empty fees
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": None,  # Gate.io returns null
                        },
                    },
                },
            }
        )

        # Second call (fallback) returns proper fees
        mock_exchange.fetch_deposit_withdraw_fees = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": {"fee": 1.0, "percentage": False},
                            "deposit": {"fee": None, "percentage": None},
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_exchange_fees("gate", mock_exchange)

        # Should have triggered fallback and returned data
        assert "USDT" in fees
        assert fees["USDT"]["TRC20"] == 1.0

        # Both APIs should have been called
        mock_exchange.fetch_currencies.assert_called_once()
        mock_exchange.fetch_deposit_withdraw_fees.assert_called_once()

    async def test_fetch_exchange_fees_no_fallback_for_other_exchanges(self):
        """Test that fallback is NOT triggered for non-Gate exchanges."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": None,  # Returns null (same as Gate)
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"bybit": mock_exchange})
        fees = await manager._fetch_exchange_fees("bybit", mock_exchange)

        # Should return empty (no fallback for Bybit)
        assert fees == {} or len(fees.get("USDT", {})) == 0

        # Only fetch_currencies should be called
        mock_exchange.fetch_currencies.assert_called_once()

    async def test_fetch_exchange_fees_no_fallback_when_gate_has_fees(self):
        """Test that fallback is NOT triggered when Gate.io returns valid fees."""
        mock_exchange = Mock()
        mock_exchange.fetch_currencies = AsyncMock(
            return_value={
                "USDT": {
                    "networks": {
                        "TRC20": {
                            "withdraw": True,
                            "active": True,
                            "fee": 1.0,  # Valid fee
                        },
                    },
                },
            }
        )

        manager = WithdrawalFeeManager(exchanges={"gate": mock_exchange})
        fees = await manager._fetch_exchange_fees("gate", mock_exchange)

        # Should use primary API data
        assert fees["USDT"]["TRC20"] == 1.0

        # Only fetch_currencies should be called
        mock_exchange.fetch_currencies.assert_called_once()
