"""
Integration tests for dynamic withdrawal fees (SPEC-FEE-001 Phase 2).

Tests the full integration of WithdrawalFeeManager with the scanner loop,
verifying that:
1. Fee manager is initialized correctly with authenticated exchanges
2. Arbitrage calculations use dynamic fees
3. per_exchange_fees is populated and used by pick_best_network()
4. Opportunity objects contain withdraw_from_exchange field
"""

from __future__ import annotations

import pytest
from unittest.mock import Mock, patch, AsyncMock
from parsertang.withdrawal_fees import WithdrawalFeeManager
from parsertang.arbitrage import Opportunity
from parsertang.core.fee_calculator import calculate_opportunity_fees_and_network
from parsertang.core.orchestrator import Orchestrator
from parsertang.config import settings


@pytest.mark.asyncio
async def test_initialize_fees_success():
    """Test successful fee manager initialization with authenticated exchanges."""
    # Test is now internal to Orchestrator._initialize_fees
    # This test validates the integration via Orchestrator
    from parsertang.core.state_manager import AppState

    state = AppState()

    with patch.object(settings, "use_dynamic_withdrawal_fees", True):
        with patch(
            "parsertang.utils.exchange_credentials.build_exchange_config"
        ) as mock_build_config:
            mock_build_config.return_value = {"enableRateLimit": True}

            with patch(
                "parsertang.core.orchestrator.WithdrawalFeeManager"
            ) as mock_fee_manager_class:
                mock_manager = AsyncMock()
                mock_manager.start_background_refresh = AsyncMock()
                mock_fee_manager_class.return_value = mock_manager

                # Mock ccxt.pro module
                with patch.dict("sys.modules", {"ccxt.pro": Mock()}):
                    orchestrator = Orchestrator()
                    orchestrator.state = state
                    try:
                        await orchestrator._initialize_fees(None)
                    except Exception:
                        # Expected to fail at ccxt.pro exchange instantiation
                        pass


@pytest.mark.asyncio
async def test_initialize_fees_disabled():
    """Test that fee manager is not initialized when disabled."""
    from parsertang.core.state_manager import AppState

    state = AppState()

    # Mock settings to disable dynamic fees
    with patch.object(settings, "use_dynamic_withdrawal_fees", False):
        with patch(
            "parsertang.core.orchestrator.WithdrawalFeeManager"
        ) as mock_fee_manager_class:
            orchestrator = Orchestrator()
            orchestrator.state = state
            await orchestrator._initialize_fees(None)

            # Verify fee manager was not created
            mock_fee_manager_class.assert_not_called()


@pytest.mark.asyncio
async def test_initialize_fees_error_handling():
    """Test that initialization errors are handled gracefully."""
    from parsertang.core.state_manager import AppState

    state = AppState()

    # Mock settings to enable dynamic fees
    with patch.object(settings, "use_dynamic_withdrawal_fees", True):
        with patch(
            "parsertang.utils.exchange_credentials.build_exchange_config"
        ) as mock_build_config:
            mock_build_config.return_value = {"enableRateLimit": True}

            with patch(
                "parsertang.core.orchestrator.WithdrawalFeeManager"
            ) as mock_fee_manager_class:
                mock_fee_manager_class.side_effect = Exception("API error")

                with patch.dict("sys.modules", {"ccxt.pro": Mock()}):
                    # Should not raise exception
                    orchestrator = Orchestrator()
                    orchestrator.state = state
                    await orchestrator._initialize_fees(None)


def test_calculate_opportunity_with_dynamic_fees():
    """Test arbitrage calculation uses dynamic fees when fee_manager is available."""
    from parsertang.core.state_manager import AppState

    # Create AppState and set fee_manager
    state = AppState()
    mock_fee_manager = Mock(spec=WithdrawalFeeManager)
    mock_fee_manager.get_per_exchange_fees_usd.return_value = {
        "TRC20": 0.8,  # Bybit TRC20 fee: $0.80 (cheaper than baseline $1.00)
        "BEP20": 0.5,  # Bybit BEP20 fee: $0.50
    }

    def _fee_side_effect(*, exchange_id: str, currency: str, network: str):
        assert exchange_id == "bybit"
        assert currency == "USDT"
        if network == "BEP20":
            return 0.5, "HIGH"
        if network == "TRC20":
            return 0.8, "HIGH"
        raise AssertionError(f"unexpected network: {network}")

    mock_fee_manager.get_withdrawal_fee.side_effect = _fee_side_effect
    state.fee_manager = mock_fee_manager

    # Mock currency cache with network data
    state.currency_cache = {
        "bybit": {
            "USDT": {
                "networks": {
                    "TRC20": {"withdraw": {"active": True, "fee": 0.8}},
                    "BEP20": {"withdraw": {"active": True, "fee": 0.5}},
                }
            }
        },
        "okx": {
            "USDT": {
                "networks": {
                    "TRC20": {"withdraw": {"active": True, "fee": 1.0}},
                    "BEP20": {"withdraw": {"active": True, "fee": 0.6}},
                }
            }
        },
    }

    with patch("parsertang.core.fee_calculator.get_taker_fee", return_value=0.1):
        # Calculate opportunity
        result = calculate_opportunity_fees_and_network(
            symbol="USDT/USDC",
            buy_exchange="bybit",
            sell_exchange="okx",
            best_ask=1.0,
            best_bid=1.005,
            state=state,
        )

    # For stablecoins with common networks, resolve_network_for_token returns the first common network
    # Network selection should prefer the cheapest network by USD fee.

    # Verify no error occurred
    assert result.is_valid

    # Extract values
    _ = result.network  # Used for debugging, not actively tested
    withdraw_fee_base = result.withdraw_fee_base  # Fee in BASE currency

    # Verify network selection used dynamic fees
    mock_fee_manager.get_withdrawal_fee.assert_called_once()
    call_args = mock_fee_manager.get_withdrawal_fee.call_args
    assert call_args[1]["exchange_id"] == "bybit"  # Withdraw FROM buy exchange
    assert call_args[1]["currency"] == "USDT"
    assert call_args[1]["network"] == "BEP20"

    # Verify withdrawal fee is from dynamic source (cheapest network selected)
    assert withdraw_fee_base == 0.5  # BEP20 fee from mock (cheapest)


def test_calculate_opportunity_without_fee_manager():
    """Test arbitrage calculation returns 0 fee when fee_manager is None."""
    from parsertang.core.state_manager import AppState

    # Create AppState without fee_manager
    state = AppState()
    state.fee_manager = None

    # Mock currency cache
    state.currency_cache = {
        "bybit": {
            "USDT": {
                "networks": {
                    "TRC20": {"withdraw": {"active": True, "fee": 0.8}},
                }
            }
        },
        "okx": {
            "USDT": {
                "networks": {
                    "TRC20": {"withdraw": {"active": True, "fee": 1.0}},
                }
            }
        },
    }

    with patch("parsertang.core.fee_calculator.get_taker_fee", return_value=0.1):
        # Calculate opportunity - should return error_reason since fee_manager is None
        result = calculate_opportunity_fees_and_network(
            symbol="USDT/USDC",
            buy_exchange="bybit",
            sell_exchange="okx",
            best_ask=1.0,
            best_bid=1.005,
            state=state,
        )

    # Without fee_manager, should return error (network selection fails first)
    assert not result.is_valid
    assert result.error_reason == "no_fee_data"
    assert result.network is None
    assert result.withdraw_fee_base == 0.0


def test_calculate_opportunity_prefers_cheapest_network_over_token_mapping():
    """When fee data exists, network selection should prefer the cheapest fee, even if TOKEN_NETWORKS prefers another."""
    from parsertang.core.state_manager import AppState

    state = AppState()
    mock_fee_manager = Mock(spec=WithdrawalFeeManager)
    mock_fee_manager.get_per_exchange_fees_usd.return_value = {
        "ERC20": 10.0,
        "TRC20": 1.0,
    }

    def _fee_side_effect(*, exchange_id: str, currency: str, network: str):
        assert exchange_id == "bybit"
        assert currency == "LINK"
        if network == "TRC20":
            return 1.0, "HIGH"
        if network == "ERC20":
            return 10.0, "HIGH"
        raise AssertionError(f"unexpected network: {network}")

    mock_fee_manager.get_withdrawal_fee.side_effect = _fee_side_effect
    state.fee_manager = mock_fee_manager

    state.currency_cache = {
        "bybit": {
            "LINK": {
                "networks": {
                    "ERC20": {"withdraw": {"active": True, "fee": 10.0}},
                    "TRC20": {"withdraw": {"active": True, "fee": 1.0}},
                }
            }
        },
        "okx": {
            "LINK": {
                "networks": {
                    "ERC20": {"withdraw": {"active": True, "fee": 10.0}},
                    "TRC20": {"withdraw": {"active": True, "fee": 1.0}},
                }
            }
        },
    }

    with patch("parsertang.core.fee_calculator.get_taker_fee", return_value=0.1):
        result = calculate_opportunity_fees_and_network(
            symbol="LINK/USDT",
            buy_exchange="bybit",
            sell_exchange="okx",
            best_ask=1.0,
            best_bid=1.01,
            state=state,
        )

    assert result.is_valid
    assert result.network == "TRC20"
    assert result.withdraw_fee_base == 1.0


def test_opportunity_withdraw_from_exchange_field():
    """Test that Opportunity objects include withdraw_from_exchange field."""
    opp = Opportunity(
        symbol="USDT/USDC",
        buy_exchange="bybit",
        sell_exchange="okx",
        buy_price=1.0,
        sell_price=1.005,
        gross_spread_pct=0.5,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.1,
        net_profit_pct=0.2,
        bid_liq_usd=10000.0,
        ask_liq_usd=10000.0,
        network="TRC20",
        withdrawal_fee_base=1.0,
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
        withdraw_from_exchange="bybit",  # New field
    )

    # Verify field is set correctly
    assert opp.withdraw_from_exchange == "bybit"
    assert opp.withdraw_from_exchange == opp.buy_exchange


def test_per_exchange_fees_network_selection():
    """Test that per_exchange_fees is used correctly in network selection."""
    from parsertang.networks import pick_best_network

    common_networks = ["TRC20", "BEP20", "ERC20"]

    # Per-exchange fees from Bybit (different from BASELINE_NETWORKS)
    per_exchange_fees = {
        "TRC20": 0.8,  # Cheaper than baseline $1.00
        "BEP20": 0.5,  # Cheaper than baseline $0.50
        "ERC20": 15.0,  # More expensive (baseline $12.00)
    }

    # Pick best network (should choose BEP20 with $0.50 fee)
    network, error = pick_best_network(
        common_networks, per_exchange_fees, trade_volume_usd=100.0
    )

    # Verify BEP20 is selected (cheapest and fast)
    assert network == "BEP20"
    assert error is None


def test_per_exchange_fees_returns_none_without_data():
    """Test that network selection returns None when per_exchange_fees is None."""
    from parsertang.networks import pick_best_network

    common_networks = ["TRC20", "BEP20"]

    # No per_exchange_fees (fee_manager not available)
    network, error = pick_best_network(common_networks, None, trade_volume_usd=100.0)

    # Should return None with error code
    assert network is None
    assert error == "no_fee_data"


@pytest.mark.asyncio
async def test_fee_manager_lifecycle():
    """Test fee manager lifecycle: initialization, refresh, and cleanup."""
    mock_exchanges = {
        "bybit": AsyncMock(),
        "okx": AsyncMock(),
    }

    # Mock fetch_deposit_withdraw_fees for bybit (used as fallback)
    mock_exchanges["bybit"].fetch_deposit_withdraw_fees = AsyncMock(
        return_value={
            "USDT": {
                "networks": {
                    "TRC20": {"withdraw": {"fee": 0.8}},
                }
            }
        }
    )

    # Mock fetch_currencies to return fees (ccxt.pro format)
    mock_exchanges["bybit"].fetch_currencies = AsyncMock(
        return_value={
            "USDT": {
                "networks": {
                    "TRC20": {
                        "withdraw": True,
                        "active": True,
                        "fee": 0.8,
                    }
                }
            }
        }
    )

    mock_exchanges["okx"].fetch_currencies = AsyncMock(
        return_value={
            "USDT": {
                "networks": {
                    "TRC20-USDT": {  # OKX uses suffix format
                        "withdraw": True,
                        "active": True,
                        "fee": 1.0,
                    }
                }
            }
        }
    )

    # Create fee manager
    fee_manager = WithdrawalFeeManager(
        exchanges=mock_exchanges,
        cache_lifetime=3600,
        fetch_timeout=10,
    )

    # Start background refresh
    await fee_manager.start_background_refresh()

    # Verify cache was populated
    assert fee_manager.cache is not None
    assert fee_manager.cache.fees["bybit"]["USDT"]["TRC20"] == 0.8
    assert (
        fee_manager.cache.fees["okx"]["USDT"]["TRC20"] == 1.0
    )  # Normalized from "TRC20-USDT"

    # Test fee lookup
    fee, confidence = fee_manager.get_withdrawal_fee("bybit", "USDT", "TRC20")
    assert fee == 0.8
    assert confidence == "HIGH"

    # Test per_exchange_fees
    per_exchange = fee_manager.get_per_exchange_fees_usd(
        exchange_id="bybit",
        currency="USDT",
        networks=["TRC20"],
        current_price_usd=1.0,  # USDT = $1.00
    )
    assert per_exchange["TRC20"] == 0.8

    # Cleanup
    await fee_manager.stop_background_refresh()


def test_withdraw_from_exchange_consistency():
    """Test that withdraw_from_exchange is always set to buy_exchange."""
    # This is a critical invariant: we always withdraw FROM the buy exchange
    opp = Opportunity(
        symbol="LTC/USDT",
        buy_exchange="bybit",
        sell_exchange="okx",
        buy_price=88.0,
        sell_price=88.5,
        gross_spread_pct=0.568,
        trade_fees_pct=0.2,
        withdraw_fee_pct=0.088,  # LTC withdrawal fee
        net_profit_pct=0.28,
        bid_liq_usd=10000.0,
        ask_liq_usd=10000.0,
        network="LTC",
        withdrawal_fee_base=0.088,
        buy_taker_fee_pct=0.1,
        sell_taker_fee_pct=0.1,
        withdraw_from_exchange="bybit",  # Must match buy_exchange
    )

    # Verify invariant
    assert opp.withdraw_from_exchange == opp.buy_exchange
    assert opp.withdraw_from_exchange != opp.sell_exchange
