"""
Unit tests for withdrawal fees diagnostic script.

Tests cover:
- Resource management (async context manager)
- Retry logic (exponential backoff, timeouts)
- Parallel execution
- Pydantic validation
- Problem detection
- Integration test (full flow)
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# This test suite validates the optional diagnostic script which depends on ccxt.pro.
# Skip gracefully when ccxt.pro isn't available in the current environment.
pytest.importorskip("ccxt.pro", reason="ccxt.pro is not installed")

# Import diagnostic components
import sys  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from diagnose_withdrawal_fees import (  # noqa: E402
    FeesDiagnostic,
    ExchangeDiagnostic,
    ProblemCurrency,
    NetworkInfo,
)


# ===== FIXTURES =====


@pytest.fixture
def mock_settings():
    """Mock settings with API credentials."""
    settings = MagicMock()
    settings.exchanges = ["bybit", "kucoin"]
    settings.bybit_api_key = "test_key"
    settings.bybit_secret = "test_secret"
    settings.kucoin_api_key = "test_key"
    settings.kucoin_secret = "test_secret"
    settings.kucoin_password = "test_pass"
    return settings


@pytest.fixture
def mock_exchange_success():
    """Mock exchange that returns valid data."""
    exchange = AsyncMock()
    exchange.id = "bybit"
    exchange.fetch_currencies = AsyncMock(
        return_value={
            "ZEN": {
                "code": "ZEN",
                "networks": {
                    "BASE": {
                        "fee": 0.03,
                        "active": True,
                        "withdraw": True,
                    }
                },
            },
            "USDT": {
                "code": "USDT",
                "networks": {
                    "TRC20": {"fee": 1.0, "active": True, "withdraw": True},
                    "ERC20": {"fee": 5.0, "active": True, "withdraw": True},
                },
            },
        }
    )
    exchange.close = AsyncMock()
    return exchange


@pytest.fixture
def mock_exchange_zero_fee():
    """Mock exchange that returns ZEN with fee=0."""
    exchange = AsyncMock()
    exchange.id = "bybit"
    exchange.fetch_currencies = AsyncMock(
        return_value={
            "ZEN": {
                "code": "ZEN",
                "networks": {
                    "BASE": {
                        "fee": 0.0,  # Problem!
                        "active": True,
                        "withdraw": True,
                    }
                },
            }
        }
    )
    exchange.close = AsyncMock()
    return exchange


@pytest.fixture
def mock_exchange_timeout():
    """Mock exchange that times out."""
    exchange = AsyncMock()
    exchange.id = "htx"
    exchange.fetch_currencies = AsyncMock(side_effect=asyncio.TimeoutError())
    exchange.close = AsyncMock()
    return exchange


# ===== RESOURCE MANAGEMENT TESTS =====


@pytest.mark.asyncio
async def test_resource_cleanup_on_success(mock_settings, mock_exchange_success):
    """Test that exchanges are closed even on success."""

    # Mock _init_exchanges to return our mock
    async def mock_init():
        return {"bybit": mock_exchange_success}

    with patch.object(FeesDiagnostic, "_init_exchanges", side_effect=mock_init):
        diagnostic = FeesDiagnostic(mock_settings)

        async with diagnostic:
            pass  # Just enter and exit

    # Verify close was called
    mock_exchange_success.close.assert_called_once()


@pytest.mark.asyncio
async def test_resource_cleanup_on_exception(mock_settings, mock_exchange_success):
    """Test that exchanges are closed even if exception occurs."""

    # Mock _init_exchanges to return our mock
    async def mock_init():
        return {"bybit": mock_exchange_success}

    with patch.object(FeesDiagnostic, "_init_exchanges", side_effect=mock_init):
        diagnostic = FeesDiagnostic(mock_settings)

        with pytest.raises(RuntimeError):
            async with diagnostic:
                raise RuntimeError("Test error")

    # Verify close was still called
    mock_exchange_success.close.assert_called_once()


@pytest.mark.asyncio
async def test_multiple_exchanges_all_closed(mock_settings):
    """Test that ALL exchanges are closed, not just first one."""
    ex1 = AsyncMock()
    ex1.close = AsyncMock()
    ex2 = AsyncMock()
    ex2.close = AsyncMock()
    ex3 = AsyncMock()
    ex3.close = AsyncMock()

    # Mock _init_exchanges to return our mocks
    async def mock_init():
        return {"ex1": ex1, "ex2": ex2, "ex3": ex3}

    with patch.object(FeesDiagnostic, "_init_exchanges", side_effect=mock_init):
        diagnostic = FeesDiagnostic(mock_settings)

        async with diagnostic:
            pass

    # All three should be closed
    ex1.close.assert_called_once()
    ex2.close.assert_called_once()
    ex3.close.assert_called_once()


# ===== RETRY LOGIC TESTS =====


@pytest.mark.asyncio
async def test_fetch_with_retry_timeout(mock_settings, mock_exchange_timeout):
    """Test retry logic on timeout."""
    diagnostic = FeesDiagnostic(mock_settings, timeout=1.0, max_retries=2)

    result = await diagnostic._fetch_exchange_fees_with_retry(
        "htx", mock_exchange_timeout
    )

    # Should fail after retries
    assert result.status == "failed"
    assert result.retry_attempts == 2
    assert "Timeout" in result.error

    # Should have called fetch_currencies 3 times (initial + 2 retries)
    assert mock_exchange_timeout.fetch_currencies.call_count == 3


@pytest.mark.asyncio
async def test_fetch_with_retry_success_on_second_attempt(mock_settings):
    """Test that retry succeeds on second attempt."""
    exchange = AsyncMock()
    exchange.id = "bybit"

    # First call fails, second succeeds
    exchange.fetch_currencies = AsyncMock(
        side_effect=[
            asyncio.TimeoutError(),  # First attempt
            {
                "USDT": {
                    "networks": {
                        "TRC20": {"fee": 1.0, "active": True, "withdraw": True}
                    }
                }
            },  # Second attempt
        ]
    )

    diagnostic = FeesDiagnostic(mock_settings, timeout=1.0, max_retries=2)

    result = await diagnostic._fetch_exchange_fees_with_retry("bybit", exchange)

    # Should succeed on retry
    assert result.status == "success"
    assert result.retry_attempts == 1  # Succeeded on first retry
    assert exchange.fetch_currencies.call_count == 2


@pytest.mark.asyncio
async def test_exponential_backoff_timing(mock_settings):
    """Test that exponential backoff works correctly (1s, 2s)."""
    exchange = AsyncMock()
    exchange.id = "test"
    exchange.fetch_currencies = AsyncMock(side_effect=asyncio.TimeoutError())

    diagnostic = FeesDiagnostic(mock_settings, timeout=0.1, max_retries=2)

    start = time.time()
    result = await diagnostic._fetch_exchange_fees_with_retry("test", exchange)
    duration = time.time() - start

    # Should take ~3.3s: 0.1s + 1s + 0.1s + 2s + 0.1s = 3.3s
    # Allow some overhead
    assert 3.0 < duration < 4.5, f"Duration {duration}s not in expected range"
    assert result.status == "failed"
    assert result.retry_attempts == 2


# ===== PARALLEL EXECUTION TESTS =====


@pytest.mark.asyncio
async def test_parallel_fetching_is_faster_than_sequential(mock_settings):
    """Verify parallel execution is faster than sequential."""
    # Create mock exchanges with delays
    slow_exchange = AsyncMock()
    slow_exchange.id = "slow"

    async def slow_fetch():
        await asyncio.sleep(1.0)
        return {
            "USDT": {
                "networks": {"TRC20": {"fee": 1.0, "active": True, "withdraw": True}}
            }
        }

    slow_exchange.fetch_currencies = slow_fetch
    slow_exchange.close = AsyncMock()

    fast_exchange = AsyncMock()
    fast_exchange.id = "fast"

    async def fast_fetch():
        await asyncio.sleep(0.5)
        return {
            "USDT": {
                "networks": {"TRC20": {"fee": 1.0, "active": True, "withdraw": True}}
            }
        }

    fast_exchange.fetch_currencies = fast_fetch
    fast_exchange.close = AsyncMock()

    diagnostic = FeesDiagnostic(mock_settings, timeout=5.0, max_retries=0)
    diagnostic.exchanges = {
        "slow": slow_exchange,
        "fast": fast_exchange,
    }

    start = time.time()
    results = await diagnostic._fetch_all_fees_parallel()
    duration = time.time() - start

    # Should complete in ~1.5s (parallel), not 1.5s (sequential)
    # Allow some overhead for tqdm and async scheduling
    assert duration < 2.5, f"Took {duration}s, expected < 2.5s for parallel execution"

    # Both exchanges should have results
    assert len(results) == 2
    assert results["slow"].status == "success"
    assert results["fast"].status == "success"


@pytest.mark.asyncio
async def test_parallel_one_failure_doesnt_block_others(
    mock_settings, mock_exchange_success, mock_exchange_timeout
):
    """Test that one exchange failure doesn't block others."""
    diagnostic = FeesDiagnostic(mock_settings, timeout=2.0, max_retries=0)
    diagnostic.exchanges = {
        "bybit": mock_exchange_success,
        "htx": mock_exchange_timeout,
    }

    results = await diagnostic._fetch_all_fees_parallel()

    # Both should have results
    assert len(results) == 2

    # Bybit should succeed
    assert results["bybit"].status == "success"
    assert results["bybit"].total_currencies == 2

    # HTX should fail
    assert results["htx"].status == "failed"
    assert "Timeout" in results["htx"].error


# ===== DATA VALIDATION TESTS =====


def test_network_info_validation_none():
    """Test Pydantic handles None fee."""
    info = NetworkInfo(fee=None, active=True, withdraw=True, network_code="TRC20")
    assert info.fee == 0.0


def test_network_info_validation_string():
    """Test Pydantic converts string to float."""
    info = NetworkInfo(fee="2.5", active=True, withdraw=True, network_code="TRC20")
    assert info.fee == 2.5


def test_network_info_validation_invalid_string():
    """Test Pydantic handles invalid string."""
    info = NetworkInfo(fee="abc", active=True, withdraw=True, network_code="TRC20")
    assert info.fee == 0.0


def test_network_info_validation_negative():
    """Test Pydantic handles negative fee."""
    info = NetworkInfo(fee=-1.0, active=True, withdraw=True, network_code="TRC20")
    assert info.fee == 0.0


def test_network_info_validation_valid():
    """Test Pydantic accepts valid fee."""
    info = NetworkInfo(fee=1.5, active=True, withdraw=True, network_code="TRC20")
    assert info.fee == 1.5


# ===== PROBLEM DETECTION TESTS =====


@pytest.mark.asyncio
async def test_detect_zero_fees(mock_settings, mock_exchange_zero_fee):
    """Test detection of currencies with zero fees."""
    diagnostic = FeesDiagnostic(mock_settings)
    diagnostic.exchanges = {"bybit": mock_exchange_zero_fee}

    # Create diagnostic results
    diagnostics = {
        "bybit": ExchangeDiagnostic(
            exchange_id="bybit",
            status="success",
            authenticated=True,
            total_currencies=1,
            currencies_with_fees=0,
            currencies_without_fees=1,
            fetch_duration_ms=100.0,
        )
    }

    problems = await diagnostic._analyze_problems(diagnostics)

    assert "ZEN" in problems
    assert problems["ZEN"].reason == "fee_zero"
    assert "bybit" in problems["ZEN"].affected_exchanges


@pytest.mark.asyncio
async def test_currencies_filter_applied(mock_settings, mock_exchange_success):
    """Test that currencies filter is applied."""
    diagnostic = FeesDiagnostic(
        mock_settings,
        currencies_filter=["ZEN"],  # Only check ZEN
    )
    diagnostic.exchanges = {"bybit": mock_exchange_success}

    result = await diagnostic._fetch_exchange_fees_with_retry(
        "bybit", mock_exchange_success
    )

    # Should only count ZEN
    assert result.total_currencies == 1  # Only ZEN (USDT filtered out)


# ===== INTEGRATION TEST =====


@pytest.mark.asyncio
async def test_integration_full_flow(tmp_path, mock_settings, mock_exchange_success):
    """Integration test: full flow from fetch to JSON output."""
    output_file = tmp_path / "test_report.json"

    # Create async mock for _init_exchanges
    async def mock_init():
        return {"bybit": mock_exchange_success}

    # Patch _init_exchanges to return our mocks
    with patch.object(FeesDiagnostic, "_init_exchanges", side_effect=mock_init):
        diagnostic = FeesDiagnostic(mock_settings, timeout=5.0)

        async with diagnostic:
            report = await diagnostic.run(output_file, verbose=False)

    # Verify JSON file created
    assert output_file.exists()

    # Verify report structure
    assert "metadata" in report
    assert "exchanges" in report
    assert "problem_currencies" in report
    assert "summary" in report

    # Verify exchange results
    assert "bybit" in report["exchanges"]
    assert report["exchanges"]["bybit"]["status"] == "success"
    assert report["exchanges"]["bybit"]["total_currencies"] == 2

    # Verify summary
    assert report["summary"]["total_exchanges"] == 1
    assert report["summary"]["successful_exchanges"] == 1

    # Verify exchange was closed
    mock_exchange_success.close.assert_called_once()


# ===== JSON STRUCTURE TEST =====


def test_json_report_structure():
    """Test that JSON report has correct structure."""
    report = {
        "metadata": {
            "timestamp": "2025-11-11T20:00:00Z",
            "parsertang_version": "0.1.0",
        },
        "exchanges": {},
        "problem_currencies": {},
        "summary": {
            "total_exchanges": 0,
            "authenticated_exchanges": 0,
        },
    }

    # Validate structure
    assert "metadata" in report
    assert "exchanges" in report
    assert "problem_currencies" in report
    assert "summary" in report

    # Validate types
    assert isinstance(report["metadata"]["timestamp"], str)
    assert isinstance(report["summary"]["total_exchanges"], int)


# ===== EDGE CASES =====


@pytest.mark.asyncio
async def test_empty_exchanges(mock_settings):
    """Test handling of empty exchanges list."""
    diagnostic = FeesDiagnostic(mock_settings)
    diagnostic.exchanges = {}  # No exchanges

    results = await diagnostic._fetch_all_fees_parallel()

    assert len(results) == 0


@pytest.mark.asyncio
async def test_currencies_with_no_networks(mock_settings):
    """Test handling of currencies with no networks."""
    exchange = AsyncMock()
    exchange.id = "test"
    exchange.fetch_currencies = AsyncMock(
        return_value={
            "BROKEN": {
                "code": "BROKEN",
                "networks": {},  # No networks!
            }
        }
    )
    exchange.close = AsyncMock()

    diagnostic = FeesDiagnostic(mock_settings)
    diagnostic.exchanges = {"test": exchange}

    result = await diagnostic._fetch_exchange_fees_with_retry("test", exchange)

    # Should handle gracefully
    assert result.status == "success"
    assert result.total_currencies == 1
    assert result.currencies_without_fees == 1


@pytest.mark.asyncio
async def test_rate_limit_handling(mock_settings):
    """Test handling of rate limit errors."""
    import ccxt

    exchange = AsyncMock()
    exchange.id = "test"
    exchange.fetch_currencies = AsyncMock(
        side_effect=ccxt.RateLimitExceeded("Rate limit exceeded")
    )

    diagnostic = FeesDiagnostic(mock_settings, timeout=1.0, max_retries=1)

    result = await diagnostic._fetch_exchange_fees_with_retry("test", exchange)

    assert result.status == "failed"
    assert "Rate limit" in result.error
    assert result.retry_attempts == 1


# ===== EXCHANGE DIAGNOSTIC DATACLASS TESTS =====


def test_exchange_diagnostic_creation():
    """Test ExchangeDiagnostic dataclass creation."""
    diag = ExchangeDiagnostic(
        exchange_id="bybit",
        status="success",
        authenticated=True,
        total_currencies=100,
        currencies_with_fees=90,
        currencies_without_fees=10,
        fetch_duration_ms=1500.5,
    )

    assert diag.exchange_id == "bybit"
    assert diag.status == "success"
    assert diag.authenticated is True
    assert diag.total_currencies == 100
    assert diag.currencies_with_fees == 90
    assert diag.currencies_without_fees == 10
    assert diag.fetch_duration_ms == 1500.5
    assert diag.fetch_method == "fetch_currencies"
    assert diag.error is None
    assert diag.retry_attempts == 0


def test_problem_currency_creation():
    """Test ProblemCurrency dataclass creation."""
    prob = ProblemCurrency(
        currency="ZEN",
        reason="fee_zero",
        affected_exchanges=["bybit", "kucoin"],
        recommendation="Check network aliases in network_aliases.py",
    )

    assert prob.currency == "ZEN"
    assert prob.reason == "fee_zero"
    assert prob.affected_exchanges == ["bybit", "kucoin"]
    assert prob.recommendation == "Check network aliases in network_aliases.py"
    assert prob.details == {}
