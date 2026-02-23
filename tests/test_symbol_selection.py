"""Unit tests for symbol_selection module.

Tests the cross-exchange symbol selection algorithm for maximizing
arbitrage opportunity overlap.
"""

import pytest
from unittest.mock import MagicMock, patch

from parsertang.symbol_selection import (
    SymbolMeta,
    build_symbol_index,
    rank_symbols_for_overlap,
    allocate_symbols_per_exchange,
    diversify_ranked_symbols,
    select_symbols,
    select_symbols_core_periphery,
)


class TestSymbolMeta:
    """Tests for SymbolMeta dataclass."""

    def test_exchange_count(self):
        """Test exchange_count property."""
        meta = SymbolMeta(
            symbol="LINK/USDT",
            exchanges={"bybit": 1000, "kucoin": 500, "mexc": 200},
        )
        assert meta.exchange_count == 3

    def test_exchange_count_empty(self):
        """Test exchange_count with no exchanges."""
        meta = SymbolMeta(symbol="LINK/USDT")
        assert meta.exchange_count == 0

    def test_aggregate_volume(self):
        """Test aggregate_volume property."""
        meta = SymbolMeta(
            symbol="LINK/USDT",
            exchanges={"bybit": 1000, "kucoin": 500, "mexc": 200},
        )
        assert meta.aggregate_volume == 1700


class TestBuildSymbolIndex:
    """Tests for build_symbol_index function."""

    def test_handles_empty_markets(self):
        """Verify graceful handling of empty input."""
        result = build_symbol_index({})
        assert result == {}

    def test_handles_empty_exchange_markets(self):
        """Verify exchange with empty market data is skipped."""
        markets = {
            "bybit": {"LINK/USDT": {"info": {"quoteVolume": 1000}}},
            "mexc": {},  # Empty
        }
        result = build_symbol_index(markets)
        assert "LINK/USDT" in result
        assert "bybit" in result["LINK/USDT"].exchanges
        assert "mexc" not in result["LINK/USDT"].exchanges

    def test_filters_stable_quotes(self):
        """Verify only stable quote currencies are included."""
        markets = {
            "bybit": {
                "LINK/USDT": {"info": {"quoteVolume": 1000}},
                "LINK/BTC": {"info": {"quoteVolume": 500}},  # Not stable quote
                "LINK/USDC": {"info": {"quoteVolume": 200}},
            },
        }
        result = build_symbol_index(markets)
        assert "LINK/USDT" in result
        assert "LINK/USDC" in result
        assert "LINK/BTC" not in result

    def test_excludes_blacklisted_assets(self):
        """Verify BTC, ETH, SOL are excluded."""
        markets = {
            "bybit": {
                "BTC/USDT": {"info": {"quoteVolume": 10000}},
                "ETH/USDT": {"info": {"quoteVolume": 5000}},
                "SOL/USDT": {"info": {"quoteVolume": 3000}},
                "LINK/USDT": {"info": {"quoteVolume": 1000}},
            },
        }
        result = build_symbol_index(markets)
        assert "LINK/USDT" in result
        assert "BTC/USDT" not in result
        assert "ETH/USDT" not in result
        assert "SOL/USDT" not in result


class TestRankSymbols:
    """Tests for rank_symbols_for_overlap function."""

    def test_prioritizes_multi_exchange(self):
        """Verify symbols on more exchanges rank higher."""
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 1000}),  # 1 exchange
            "BBB/USDT": SymbolMeta(
                "BBB/USDT", {"bybit": 500, "mexc": 500}
            ),  # 2 exchanges
            "CCC/USDT": SymbolMeta(
                "CCC/USDT", {"bybit": 100, "mexc": 100, "kucoin": 100}
            ),  # 3 exchanges
        }
        ranked = rank_symbols_for_overlap(index)
        assert ranked[0].symbol == "CCC/USDT"  # 3 exchanges first
        assert ranked[1].symbol == "BBB/USDT"  # 2 exchanges second
        assert ranked[2].symbol == "AAA/USDT"  # 1 exchange last

    def test_deterministic_sort(self):
        """Verify same input produces same output (deterministic)."""
        index = {
            "BBB/USDT": SymbolMeta("BBB/USDT", {"bybit": 100, "mexc": 100}),
            "AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 100, "kucoin": 100}),
        }
        ranked1 = rank_symbols_for_overlap(index)
        ranked2 = rank_symbols_for_overlap(index)
        # Same exchange count, same volume, sort by symbol name
        assert ranked1[0].symbol == ranked2[0].symbol == "AAA/USDT"
        assert ranked1[1].symbol == ranked2[1].symbol == "BBB/USDT"

    def test_sorts_by_volume_secondary(self):
        """Verify volume is secondary sort key."""
        index = {
            "AAA/USDT": SymbolMeta(
                "AAA/USDT", {"bybit": 100, "mexc": 100}
            ),  # 200 total
            "BBB/USDT": SymbolMeta(
                "BBB/USDT", {"bybit": 500, "mexc": 500}
            ),  # 1000 total
        }
        ranked = rank_symbols_for_overlap(index)
        # Same exchange count, higher volume first
        assert ranked[0].symbol == "BBB/USDT"
        assert ranked[1].symbol == "AAA/USDT"


class TestDiversifyRankedSymbols:
    def test_injects_mid_ranked_symbols(self):
        """Diversity should pull some symbols from below the top ranks."""
        index = {
            # All multi-ex (bybit+okx). Volume desc is the rank driver.
            f"S{i}/USDT": SymbolMeta(
                f"S{i}/USDT",
                {"bybit": float(10_000 - i), "okx": float(10_000 - i)},
            )
            for i in range(10)
        }
        ranked = rank_symbols_for_overlap(index)
        diversified = diversify_ranked_symbols(
            ranked,
            target_unique=4,
            diversify_fraction=0.5,
            pool_multiplier=5,
        )

        top4 = [m.symbol for m in diversified[:4]]
        assert "S0/USDT" in top4
        assert "S1/USDT" in top4

        original_pos = {m.symbol: i for i, m in enumerate(ranked)}
        assert any(original_pos[sym] >= 4 for sym in top4)


class TestAllocateSymbols:
    """Tests for allocate_symbols_per_exchange function."""

    def test_handles_empty_input(self):
        """Verify empty input returns empty output."""
        result = allocate_symbols_per_exchange({}, [], max_per_exchange=30)
        assert result == {}

    def test_handles_zero_max(self):
        """Verify max_per_exchange=0 returns empty allocation."""
        index = {"AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 1000})}
        ranked = list(index.values())
        result = allocate_symbols_per_exchange(index, ranked, max_per_exchange=0)
        assert result == {}

    def test_fills_overlap_first(self):
        """Verify multi-exchange symbols are allocated before single-exchange."""
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 1000}),  # Single exchange
            "BBB/USDT": SymbolMeta("BBB/USDT", {"bybit": 500, "mexc": 500}),  # Multi
            "CCC/USDT": SymbolMeta("CCC/USDT", {"mexc": 200}),  # Single exchange
        }
        ranked = rank_symbols_for_overlap(index)
        result = allocate_symbols_per_exchange(index, ranked, max_per_exchange=2)

        # BBB/USDT should be on both bybit and mexc
        assert "BBB/USDT" in result.get("bybit", [])
        assert "BBB/USDT" in result.get("mexc", [])

    def test_respects_max_per_exchange(self):
        """Verify per-exchange cap is respected."""
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 1000, "mexc": 1000}),
            "BBB/USDT": SymbolMeta("BBB/USDT", {"bybit": 500, "mexc": 500}),
            "CCC/USDT": SymbolMeta("CCC/USDT", {"bybit": 200, "mexc": 200}),
        }
        ranked = rank_symbols_for_overlap(index)
        result = allocate_symbols_per_exchange(index, ranked, max_per_exchange=2)

        assert len(result.get("bybit", [])) <= 2
        assert len(result.get("mexc", [])) <= 2

    def test_caps_multi_exchange_to_min_overlap_by_default(self):
        """Symbols on 3+ exchanges should be subscribed on 2 by default (pairwise)."""
        index = {
            "AAA/USDT": SymbolMeta(
                "AAA/USDT", {"bybit": 1000.0, "okx": 900.0, "mexc": 800.0}
            ),
        }
        ranked = rank_symbols_for_overlap(index)
        result = allocate_symbols_per_exchange(index, ranked, max_per_exchange=10)

        assert "AAA/USDT" in result.get("bybit", [])
        assert "AAA/USDT" in result.get("okx", [])
        assert "AAA/USDT" not in result.get("mexc", [])

    def test_handles_all_single_exchange(self):
        """Verify graceful fallback when no multi-exchange symbols exist."""
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"bybit": 1000}),
            "BBB/USDT": SymbolMeta("BBB/USDT", {"mexc": 500}),
            "CCC/USDT": SymbolMeta("CCC/USDT", {"kucoin": 200}),
        }
        ranked = rank_symbols_for_overlap(index)
        result = allocate_symbols_per_exchange(index, ranked, max_per_exchange=30)

        assert "AAA/USDT" in result.get("bybit", [])
        assert "BBB/USDT" in result.get("mexc", [])
        assert "CCC/USDT" in result.get("kucoin", [])

    def test_does_not_couple_exchange_caps(self):
        """A tight cap on one exchange must not block overlap on others."""
        index = {
            "AAA/USDT": SymbolMeta(
                "AAA/USDT", {"bybit": 1000.0, "okx": 900.0, "mexc": 100.0}
            ),
        }
        ranked = rank_symbols_for_overlap(index)
        result = allocate_symbols_per_exchange(
            index,
            ranked,
            max_per_exchange=2,
            exchange_limits={"bybit": 2, "okx": 2, "mexc": 0},
        )

        assert "AAA/USDT" in result.get("bybit", [])
        assert "AAA/USDT" in result.get("okx", [])
        assert "AAA/USDT" not in result.get("mexc", [])


class TestSelectSymbols:
    """Tests for select_symbols high-level function."""

    def test_cross_exchange_strategy(self):
        """Verify cross_exchange strategy uses overlap algorithm."""
        markets = {
            "bybit": {
                "LINK/USDT": {"info": {"quoteVolume": 1000}},
                "ADA/USDT": {"info": {"quoteVolume": 500}},
            },
            "mexc": {
                "LINK/USDT": {"info": {"quoteVolume": 800}},
                "DOGE/USDT": {"info": {"quoteVolume": 300}},
            },
        }
        result = select_symbols(markets, max_per_exchange=30, strategy="cross_exchange")

        # LINK/USDT is on both exchanges, should be allocated to both
        assert "LINK/USDT" in result.get("bybit", [])
        assert "LINK/USDT" in result.get("mexc", [])

    def test_local_volume_strategy_returns_empty(self):
        """Verify local_volume strategy returns empty (handled by main.py)."""
        markets = {
            "bybit": {"LINK/USDT": {"info": {"quoteVolume": 1000}}},
        }
        result = select_symbols(markets, max_per_exchange=30, strategy="local_volume")
        # local_volume is handled by main.py, select_symbols returns empty
        assert result == {}


class TestSelectSymbolsCorePeriphery:
    """Tests for core+periphery symbol selection strategy."""

    def test_periphery_prioritizes_core_symbols(self):
        """Periphery exchanges should prefer symbols already chosen in core."""
        markets = {
            "bybit": {
                "AAA/USDT": {"info": {"quoteVolume": 1000}},
                "BBB/USDT": {"info": {"quoteVolume": 900}},
                "CCC/USDT": {"info": {"quoteVolume": 100}},
            },
            "okx": {
                "AAA/USDT": {"info": {"quoteVolume": 800}},
                "BBB/USDT": {"info": {"quoteVolume": 700}},
                "DDD/USDT": {"info": {"quoteVolume": 200}},
            },
            "kucoin": {
                "AAA/USDT": {"info": {"quoteVolume": 600}},
                "EEE/USDT": {"info": {"quoteVolume": 300}},
            },
            "gate": {
                "AAA/USDT": {"info": {"quoteVolume": 500}},
                "BBB/USDT": {"info": {"quoteVolume": 400}},
                "XXX/USDT": {"info": {"quoteVolume": 50}},
            },
            "mexc": {
                "AAA/USDT": {"info": {"quoteVolume": 450}},
                "CCC/USDT": {"info": {"quoteVolume": 200}},
                "YYY/USDT": {"info": {"quoteVolume": 60}},
            },
        }

        core = ["bybit", "okx", "kucoin"]
        periphery = ["gate", "mexc"]
        exchange_limits = {
            "bybit": 2,
            "okx": 2,
            "kucoin": 2,
            "gate": 2,
            "mexc": 2,
        }

        result = select_symbols_core_periphery(
            markets,
            max_per_exchange=2,
            core_exchanges=core,
            periphery_exchanges=periphery,
            exchange_limits=exchange_limits,
        )

        # Gate should prioritize core symbols AAA/BBB
        assert result["gate"] == ["AAA/USDT", "BBB/USDT"]
        # MEXC should include AAA/USDT from core when available
        assert "AAA/USDT" in result["mexc"]

    def test_core_respects_limits(self):
        """Core allocation should fill up to per-exchange limits."""
        markets = {
            "bybit": {
                "AAA/USDT": {"info": {"quoteVolume": 1000}},
                "BBB/USDT": {"info": {"quoteVolume": 900}},
                "CCC/USDT": {"info": {"quoteVolume": 800}},
            },
            "okx": {
                "AAA/USDT": {"info": {"quoteVolume": 800}},
                "BBB/USDT": {"info": {"quoteVolume": 700}},
                "DDD/USDT": {"info": {"quoteVolume": 200}},
            },
            "kucoin": {
                "AAA/USDT": {"info": {"quoteVolume": 600}},
                "EEE/USDT": {"info": {"quoteVolume": 300}},
            },
        }

        core = ["bybit", "okx", "kucoin"]
        exchange_limits = {"bybit": 2, "okx": 2, "kucoin": 2}

        result = select_symbols_core_periphery(
            markets,
            max_per_exchange=2,
            core_exchanges=core,
            periphery_exchanges=[],
            exchange_limits=exchange_limits,
        )

        assert len(result["bybit"]) == 2
        assert len(result["okx"]) == 2
        assert len(result["kucoin"]) == 2

    def test_periphery_fallback_when_no_core_overlap(self, caplog):
        """Periphery should fall back to local symbols when no core overlap exists."""
        markets = {
            "bybit": {
                "AAA/USDT": {"info": {"quoteVolume": 1000}},
            },
            "okx": {
                "AAA/USDT": {"info": {"quoteVolume": 900}},
            },
            "kucoin": {
                "AAA/USDT": {"info": {"quoteVolume": 800}},
            },
            "mexc": {
                "MMM/USDT": {"info": {"quoteVolume": 500}},
                "NNN/USDT": {"info": {"quoteVolume": 100}},
            },
        }

        core = ["bybit", "okx", "kucoin"]
        periphery = ["mexc"]
        exchange_limits = {"bybit": 1, "okx": 1, "kucoin": 1, "mexc": 2}

        with caplog.at_level("INFO"):
            result = select_symbols_core_periphery(
                markets,
                max_per_exchange=2,
                core_exchanges=core,
                periphery_exchanges=periphery,
                exchange_limits=exchange_limits,
            )

        assert result["mexc"] == ["MMM/USDT", "NNN/USDT"]
        assert "periphery fallback" in caplog.text


class TestVolumeBasedRanking:
    """Tests for SPEC-VOL-001: Volume-Based Symbol Ranking."""

    def test_rank_symbols_by_volume(self):
        """Symbols with higher volume rank first."""
        index = {
            "LOW/USDT": SymbolMeta("LOW/USDT", {"ex1": 1000}),
            "HIGH/USDT": SymbolMeta("HIGH/USDT", {"ex1": 1_000_000}),
        }
        ranked = rank_symbols_for_overlap(index)
        assert ranked[0].symbol == "HIGH/USDT"

    def test_rank_symbols_alphabetic_fallback_when_no_volume(self):
        """Without volume data sorting is alphabetic (explicit behavior)."""
        index = {
            "ZZZ/USDT": SymbolMeta("ZZZ/USDT", {"ex1": 0}),
            "AAA/USDT": SymbolMeta("AAA/USDT", {"ex1": 0}),
        }
        ranked = rank_symbols_for_overlap(index)
        assert ranked[0].symbol == "AAA/USDT"

    def test_merge_volume_updates_symbol_index(self):
        """Volume data correctly merges into index."""
        from parsertang.core.orchestrator import Orchestrator

        orchestrator = Orchestrator()
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"ex1": 0}),
            "BBB/USDT": SymbolMeta("BBB/USDT", {"ex1": 0}),
        }
        volumes = {
            "ex1": {"AAA/USDT": 50000, "BBB/USDT": 100000},
        }

        updated = orchestrator._merge_volume_into_index(index, volumes)

        assert updated == 2
        assert index["AAA/USDT"].exchanges["ex1"] == 50000
        assert index["BBB/USDT"].exchanges["ex1"] == 100000

    def test_merge_volume_skips_zero_volume(self):
        """Zero volume values are not merged."""
        from parsertang.core.orchestrator import Orchestrator

        orchestrator = Orchestrator()
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"ex1": 0}),
        }
        volumes = {
            "ex1": {"AAA/USDT": 0},
        }

        updated = orchestrator._merge_volume_into_index(index, volumes)

        assert updated == 0
        assert index["AAA/USDT"].exchanges["ex1"] == 0

    def test_merge_volume_skips_unknown_symbols(self):
        """Unknown symbols in volume data are skipped."""
        from parsertang.core.orchestrator import Orchestrator

        orchestrator = Orchestrator()
        index = {
            "AAA/USDT": SymbolMeta("AAA/USDT", {"ex1": 0}),
        }
        volumes = {
            "ex1": {"UNKNOWN/USDT": 50000},
        }

        updated = orchestrator._merge_volume_into_index(index, volumes)

        assert updated == 0


class TestVolumeRetryLogic:
    """Tests for volume fetching retry logic."""

    @pytest.mark.asyncio
    async def test_fetch_volume_retry_on_failure(self):
        """Retry logic works on temporary failures."""
        from parsertang.core.orchestrator import Orchestrator

        orchestrator = Orchestrator()

        # Mock gateway with exchange that fails twice then succeeds
        mock_client = MagicMock()
        mock_client.markets = {"AAA/USDT": {"spot": True}}

        call_count = 0

        def fetch_tickers_side_effect(symbols):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Network error attempt {call_count}")
            return {"AAA/USDT": {"quoteVolume": 50000}}

        mock_client.fetch_tickers = fetch_tickers_side_effect

        mock_gateway = MagicMock()
        mock_gateway.exchanges = {"ex1": mock_client}
        orchestrator.gateway = mock_gateway

        # Mock to_thread to run synchronously and patch settings in orchestrator module
        async def mock_to_thread(func, *args):
            return func(*args)

        with (
            patch("parsertang.core.orchestrator.settings") as mock_settings,
            patch("parsertang.core.orchestrator.asyncio.to_thread", mock_to_thread),
        ):
            mock_settings.exchanges = ["ex1"]

            result = await orchestrator._fetch_volume_all_with_retry(max_retries=3)

        assert call_count == 3  # 2 failures + 1 success
        assert "ex1" in result
        assert result["ex1"]["AAA/USDT"] == 50000

    @pytest.mark.asyncio
    async def test_fetch_volume_partial_failure(self):
        """Partial failures don't block other exchanges."""
        from parsertang.core.orchestrator import Orchestrator

        orchestrator = Orchestrator()

        # ex1 always fails, ex2 succeeds
        mock_client_fail = MagicMock()
        mock_client_fail.markets = {"AAA/USDT": {"spot": True}}
        mock_client_fail.fetch_tickers = MagicMock(
            side_effect=Exception("Exchange down")
        )

        mock_client_ok = MagicMock()
        mock_client_ok.markets = {"BBB/USDT": {"spot": True}}
        mock_client_ok.fetch_tickers = MagicMock(
            return_value={"BBB/USDT": {"quoteVolume": 100000}}
        )

        mock_gateway = MagicMock()
        mock_gateway.exchanges = {"ex1": mock_client_fail, "ex2": mock_client_ok}
        orchestrator.gateway = mock_gateway

        async def mock_to_thread(func, *args):
            return func(*args)

        with (
            patch("parsertang.core.orchestrator.settings") as mock_settings,
            patch("parsertang.core.orchestrator.asyncio.to_thread", mock_to_thread),
        ):
            mock_settings.exchanges = ["ex1", "ex2"]

            result = await orchestrator._fetch_volume_all_with_retry(max_retries=1)

        # ex1 failed but ex2 succeeded
        assert result["ex1"] == {}
        assert result["ex2"]["BBB/USDT"] == 100000
