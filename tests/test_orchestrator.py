"""Unit tests for core/orchestrator.py."""

import pytest
from unittest.mock import MagicMock, patch


class TestBuildProxyConfig:
    """Tests for Orchestrator._build_proxy_config()."""

    def test_no_proxy_returns_none(self):
        """When no proxy configured, returns None."""
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.http_proxy = None
            mock_settings.https_proxy = None

            orch = Orchestrator()
            result = orch._build_proxy_config()

            assert result is None

    def test_http_proxy_only(self):
        """When only HTTP proxy configured."""
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.http_proxy = "http://proxy:8080"
            mock_settings.https_proxy = None

            orch = Orchestrator()
            result = orch._build_proxy_config()

            assert result == {"http": "http://proxy:8080"}

    def test_https_proxy_only(self):
        """When only HTTPS proxy configured."""
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.http_proxy = None
            mock_settings.https_proxy = "https://proxy:8443"

            orch = Orchestrator()
            result = orch._build_proxy_config()

            assert result == {"https": "https://proxy:8443"}

    def test_both_proxies(self):
        """When both HTTP and HTTPS proxies configured."""
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.http_proxy = "http://proxy:8080"
            mock_settings.https_proxy = "https://proxy:8443"

            orch = Orchestrator()
            result = orch._build_proxy_config()

            assert result == {
                "http": "http://proxy:8080",
                "https": "https://proxy:8443",
            }


class TestFilterStableSymbols:
    """Tests for Orchestrator._filter_stable_symbols()."""

    def test_filters_stable_quote_only(self):
        """Only symbols with stable quote currency pass filter."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()
        orch.gateway = MagicMock()
        orch.gateway.is_stable_quote.side_effect = lambda s: s.endswith("/USDT")

        markets = {
            "BTC/USDT": {"spot": True},
            "ETH/USDT": {"spot": True},
            "BTC/EUR": {"spot": True},  # Not stable quote
        }

        with patch("parsertang.core.orchestrator.EXCLUDED_BASE_ASSETS", set()):
            result = orch._filter_stable_symbols(markets)

        assert "BTC/USDT" in result
        assert "ETH/USDT" in result
        assert "BTC/EUR" not in result

    def test_filters_spot_only(self):
        """Only spot markets pass filter (not futures/swaps)."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()
        orch.gateway = MagicMock()
        orch.gateway.is_stable_quote.return_value = True

        markets = {
            "BTC/USDT": {"spot": True},
            "BTC/USDT:USDT": {"swap": True},  # Perpetual swap
            "ETH/USDT": {"future": True},  # Future
        }

        with patch("parsertang.core.orchestrator.EXCLUDED_BASE_ASSETS", set()):
            result = orch._filter_stable_symbols(markets)

        assert "BTC/USDT" in result
        assert "BTC/USDT:USDT" not in result
        assert "ETH/USDT" not in result

    def test_filters_excluded_base_assets(self):
        """Symbols with excluded base assets are filtered out."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()
        orch.gateway = MagicMock()
        orch.gateway.is_stable_quote.return_value = True

        markets = {
            "BTC/USDT": {"spot": True},
            "USDT/USDC": {"spot": True},  # USDT is excluded
        }

        with patch(
            "parsertang.core.orchestrator.EXCLUDED_BASE_ASSETS", {"USDT", "USDC"}
        ):
            result = orch._filter_stable_symbols(markets)

        assert "BTC/USDT" in result
        assert "USDT/USDC" not in result


class TestSortByVolume:
    """Tests for Orchestrator._sort_by_volume()."""

    def test_sorts_by_volume_descending(self):
        """Symbols are sorted by 24h volume (highest first)."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()

        symbols = ["LOW/USDT", "HIGH/USDT", "MED/USDT"]
        markets = {
            "LOW/USDT": {"info": {"quoteVolume": "100"}},
            "HIGH/USDT": {"info": {"quoteVolume": "10000"}},
            "MED/USDT": {"info": {"quoteVolume": "1000"}},
        }

        result = orch._sort_by_volume(symbols, markets)

        assert result == ["HIGH/USDT", "MED/USDT", "LOW/USDT"]

    def test_handles_missing_volume(self):
        """Symbols without volume data get 0 volume."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()

        symbols = ["NO_VOL/USDT", "HAS_VOL/USDT"]
        markets = {
            "NO_VOL/USDT": {},
            "HAS_VOL/USDT": {"info": {"quoteVolume": "1000"}},
        }

        result = orch._sort_by_volume(symbols, markets)

        assert result[0] == "HAS_VOL/USDT"
        assert result[1] == "NO_VOL/USDT"

    def test_stable_sort_by_name_on_equal_volume(self):
        """Symbols with equal volume are sorted alphabetically."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()

        symbols = ["ZZZ/USDT", "AAA/USDT", "MMM/USDT"]
        markets = {
            "ZZZ/USDT": {"info": {"quoteVolume": "100"}},
            "AAA/USDT": {"info": {"quoteVolume": "100"}},
            "MMM/USDT": {"info": {"quoteVolume": "100"}},
        }

        result = orch._sort_by_volume(symbols, markets)

        assert result == ["AAA/USDT", "MMM/USDT", "ZZZ/USDT"]


class TestCreateBackgroundTask:
    """Tests for Orchestrator._create_background_task()."""

    @pytest.mark.asyncio
    async def test_task_completes_normally(self):
        """Background task runs to completion."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()
        completed = False

        async def simple_coro():
            nonlocal completed
            completed = True

        task = orch._create_background_task(simple_coro(), "test_task")
        await task

        assert completed

    @pytest.mark.asyncio
    async def test_task_handles_exception(self):
        """Background task logs exception but doesn't crash."""
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()

        async def failing_coro():
            raise ValueError("Test error")

        task = orch._create_background_task(failing_coro(), "failing_task")

        # Should not raise - exception is logged internally
        await task

    @pytest.mark.asyncio
    async def test_task_handles_cancellation(self):
        """Background task handles cancellation gracefully."""
        import asyncio
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()

        async def long_running():
            await asyncio.sleep(10)

        task = orch._create_background_task(long_running(), "long_task")
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task


class TestResolveWsExchanges:
    def test_defaults_to_exchanges(self):
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.exchanges = ["bybit", "okx"]
            mock_settings.ws_exchanges = None

            orch = Orchestrator()
            assert orch._resolve_ws_exchanges() == ["bybit", "okx"]

    def test_uses_ws_exchanges_override(self):
        from parsertang.core.orchestrator import Orchestrator

        with patch("parsertang.core.orchestrator.settings") as mock_settings:
            mock_settings.exchanges = ["bybit", "okx"]
            mock_settings.ws_exchanges = ["bybit"]

            orch = Orchestrator()
            assert orch._resolve_ws_exchanges() == ["bybit"]


class TestRestSnapshotLogging:
    def test_logs_rest_snapshot_start(self, caplog):
        from parsertang.core.orchestrator import Orchestrator

        orch = Orchestrator()
        symbols = {"htx": ["A/USDT", "B/USDT"], "okx": []}

        with caplog.at_level("INFO"):
            orch._log_rest_snapshot_start(symbols, interval_seconds=30)

        assert "REST SNAPSHOT | start" in caplog.text
        assert "htx=2" in caplog.text
        assert "okx=0" in caplog.text

    def test_formats_rest_snapshot_summary(self):
        from parsertang.core.orchestrator import Orchestrator

        ok_counts = {"htx": 3, "okx": 1}
        err_counts = {"htx": 2}

        summary = Orchestrator._format_rest_snapshot_summary(ok_counts, err_counts)

        assert "htx=ok:3 err:2" in summary
        assert "okx=ok:1 err:0" in summary

    def test_should_log_rest_snapshot_interval(self):
        from parsertang.core.orchestrator import Orchestrator

        assert Orchestrator._should_log_rest_snapshot(100.0, 0.0, 300) is False
        assert Orchestrator._should_log_rest_snapshot(400.0, 0.0, 300) is True
        assert Orchestrator._should_log_rest_snapshot(10.0, 0.0, 0) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
