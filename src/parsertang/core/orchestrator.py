"""Application orchestration and lifecycle management.

Manages:
- Exchange initialization (REST and WebSocket)
- Background task lifecycle (metrics, cleanup, metadata refresh)
- Graceful shutdown
- Signal handling (SIGINT, SIGTERM)

Orchestrator owns the main async loop and coordinates all subsystems.
All state is encapsulated in AppState (passed to components).

Architecture:
    main.py → Orchestrator.run()
        ↓
    Orchestrator initializes: AppState, exchanges, streams, trader
        ↓
    Orchestrator starts background tasks: metrics, cleanup, WS
        ↓
    Orchestrator handles shutdown: cancel tasks, close connections
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import logging
import time
from typing import Any
from typing import Awaitable, Callable, Dict, List

from parsertang.alerts import AlertService, SimpleBot
from parsertang.adaptive_symbol_limits import AdaptiveSymbolLimiter, HealthSnapshot
from parsertang.config import (
    EXCLUDED_BASE_ASSETS,
    Settings,
    WS_ID_ALIASES,
    get_exchange_symbol_limits,
    settings,
)
from parsertang.fee_statistics import FeeStatistics, daily_fee_report_task
from parsertang.symbol_selection import (
    SymbolMeta,
    build_symbol_index,
    rank_symbols_for_overlap,
    diversify_ranked_symbols,
    allocate_symbols_per_exchange,
    select_symbols_core_periphery,
)
from parsertang.core.metrics_logger import (
    cleanup_stale_data,
    log_alert_funnel_summary,
    log_liquidity_summary,
    log_fee_validation_summary,
    log_spread_distribution,
    log_validation_summary,
    log_trader_stats,
    log_ws_health,
)
from parsertang.ws_guard import WSGuard, build_incident_snapshot, guard_once
from parsertang.symbol_refresh import RefreshInputs, should_refresh_symbols
from parsertang.core.orderbook_processor import on_orderbook_update
from parsertang.core.opportunity_evaluator import evaluate_arbitrage_for_symbol
from parsertang.liquidity import liquidity_usd_within_window
from parsertang.slippage import estimate_buy_slippage_pct, estimate_sell_slippage_pct
from parsertang.core.state_manager import AppState
from parsertang.exchanges import ExchangeGateway
from parsertang.health_monitor import CircuitOpenError, ExchangeHealthMonitor
from parsertang.logging_conf import setup_logging
from parsertang.streams import Streams
from parsertang.trader import SimpleTrader
from parsertang.withdrawal_fees import WithdrawalFeeManager
from parsertang.refresh_trace import RefreshTrace
from parsertang.build_info import log_build_info
from parsertang.rest_snapshot_guard import (
    RestSnapshotInputs,
    should_restart_rest_snapshot,
)
from parsertang.v2.shadow import ShadowPipeline

logger = logging.getLogger(__name__)

REFRESH_TRACE_TIMEOUT_SECONDS = 30


class Orchestrator:
    """Main application orchestrator.

    Responsibilities:
    - Initialize all subsystems (exchanges, streams, trader, alerts)
    - Start background monitoring tasks
    - Manage application lifecycle
    - Coordinate graceful shutdown

    State Management:
    - All shared state lives in AppState instance
    - Components receive AppState via dependency injection
    - No global variables (thread-safe by design)
    """

    def __init__(self) -> None:
        """Initialize orchestrator (does not start services)."""
        self.state: AppState | None = None
        self.gateway: ExchangeGateway | None = None
        self.streams: Streams | None = None
        self.health_monitor: ExchangeHealthMonitor | None = None
        self.background_tasks: List[asyncio.Task] = []
        self._symbol_refresh_last_ts: float = 0.0
        self._allowlist_last_mtime: float = 0.0
        self._allowlist_refresh_last_ts: float = 0.0
        self._refresh_trace: RefreshTrace | None = None
        self._refresh_trace_reported: bool = False
        self._refresh_trace_timeout_task: asyncio.Task | None = None
        self._ws_recover_event = asyncio.Event()
        self._ws_recover_future: asyncio.Future[bool] | None = None
        self._ws_recover_pending: bool = False
        self._ws_recover_timeout_task: asyncio.Task | None = None
        self._ws_recover_timeout_seconds: int = 30
        self._ws_recover_expected_exchanges: set[str] | None = None
        self._ws_tasks: dict[str, asyncio.Task] = {}
        self._ws_clients: dict[str, Any] = {}
        self._ws_last_restart: dict[str, float] = {}
        self._ws_exchange_recover_futures: dict[str, asyncio.Future[bool]] = {}
        self._ws_symbols_per_exchange: dict[str, list[str]] = {}
        self._ws_on_update: Callable[[str, str, dict], None] | None = None
        self._ws_l0_timeout_seconds: int = 30
        self._ws_l0_cooldown_seconds: int = 600
        self._rest_restart_last_ts: float = 0.0
        self._symbol_ramp_limiter: AdaptiveSymbolLimiter | None = None
        # V2 shadow pipeline (side-by-side, no prod impact)
        self._shadow_pipeline: ShadowPipeline | None = None

    @staticmethod
    def _should_use_native_ws(settings_obj: Settings) -> bool:
        return bool(settings_obj.ws_native_enabled)

    @staticmethod
    def _build_native_ws_clients(exchanges: list[str]):
        from parsertang.ws_native.clients import BybitClient, MexcClient, OkxClient

        mapping = {"okx": OkxClient, "bybit": BybitClient, "mexc": MexcClient}
        return {ex: mapping[ex]() for ex in exchanges if ex in mapping}

    async def run(self) -> None:
        """Main entry point — runs entire application lifecycle.

        Workflow:
        1. Setup logging
        2. Initialize exchanges and health monitoring
        3. Load markets and select symbols
        4. Initialize fees, alerts, trader
        5. Start background tasks (metrics, cleanup)
        6. Subscribe to orderbooks (WS or REST polling)
        7. Wait for completion or cancellation
        8. Cleanup on shutdown
        """
        setup_logging()
        log_build_info(logger)
        logger.info("Starting Parsertang")

        # Log excluded assets (hardcoded, not configurable)
        logger.info(
            "Excluded base assets (hardcoded): %s",
            ", ".join(sorted(EXCLUDED_BASE_ASSETS)),
        )
        self._symbol_ramp_limiter = self._build_symbol_ramp_limiter()

        # Proxy configuration
        proxy_config = self._build_proxy_config()
        if proxy_config:
            logger.info(
                "PROXY CONFIG | REST API will use proxy: %s",
                proxy_config.get("https", proxy_config.get("http", "N/A")),
            )

        # Initialize circuit breaker health monitor
        self.health_monitor = ExchangeHealthMonitor(settings)
        logger.info(
            "Circuit Breaker initialized: enabled=%s threshold=%d timeout=%ds",
            settings.circuit_breaker_enabled,
            settings.circuit_failure_threshold,
            settings.circuit_recovery_timeout_seconds,
        )

        # Initialize exchange gateway (REST API)
        self.gateway = ExchangeGateway(
            settings.exchanges,
            settings,
            proxy_config=proxy_config or None,
            health_monitor=self.health_monitor,
        )

        # Load markets from all exchanges
        markets = self.gateway.load_markets()
        self._log_market_status(markets)

        # Resolve WS exchanges (optional override for REST-only exchanges)
        ws_exchanges = self._resolve_ws_exchanges()
        markets_ws: Dict[str, Dict] = {}
        for ex_id in ws_exchanges:
            m = markets.get(ex_id)
            if m:
                markets_ws[ex_id] = m

        # Select symbols per exchange (cross_exchange or local_volume strategy)
        symbols_per_exchange, all_symbols = await self._select_symbols(markets_ws)
        rest_snapshot_symbols = self._select_rest_snapshot_symbols(
            markets, symbols_per_exchange
        )
        self._symbol_refresh_last_ts = time.time()

        # Initialize AppState (simple constructor, services set later)
        self.state = AppState()
        self.state.gateway = self.gateway

        # Initialize fee statistics (always, tracking is cheap)
        self.state.fee_stats = FeeStatistics()

        # Apply initial symbol allocation to state
        await self._apply_symbol_selection(symbols_per_exchange)

        # Start metadata refresh (background task)
        await self._start_metadata_refresh()

        # Initialize withdrawal fee manager
        await self._initialize_fees(proxy_config)

        # Initialize alerts
        self.state.alert_service = AlertService()

        # Initialize trader (if enabled)
        await self._initialize_trader()

        # Initialize Telegram bot (if enabled)
        await self._initialize_bot()

        # Start background monitoring tasks
        self._start_background_tasks()

        # Initialize V2 shadow pipeline (side-by-side observability)
        from parsertang.v2.shadow import (
            ShadowPipeline,
        )  # local import to avoid circulars

        log_level = ShadowPipeline.level_from_str(settings.v2_shadow_log_level)
        self._shadow_pipeline = (
            None
            if settings.v2_shadow_log_level.upper() == "OFF"
            else ShadowPipeline(log_level=log_level)
        )

        # Subscribe to orderbooks (WS or REST polling mode)
        if settings.ws_enabled:
            await self._run_websocket_mode(
                symbols_per_exchange,
                rest_snapshot_symbols,
                ws_exchanges=ws_exchanges,
                markets_ws=markets_ws,
            )
        else:
            await self._run_rest_mode(symbols_per_exchange)

        # Cleanup on exit
        await self._cleanup()

    def _build_proxy_config(self) -> Dict[str, str] | None:
        """Build proxy configuration from settings.

        Returns:
            Dict with 'http' and/or 'https' keys, or None if no proxy configured
        """
        proxy_config = {}
        if settings.http_proxy:
            proxy_config["http"] = settings.http_proxy
        if settings.https_proxy:
            proxy_config["https"] = settings.https_proxy
        return proxy_config or None

    def _resolve_ws_exchanges(self) -> list[str]:
        if settings.ws_exchanges:
            return list(settings.ws_exchanges)
        return list(settings.exchanges)

    def _log_rest_snapshot_start(
        self,
        symbols_per_exchange: Dict[str, List[str]],
        *,
        interval_seconds: int,
    ) -> None:
        counts = {
            ex_id: len(symbols) for ex_id, symbols in symbols_per_exchange.items()
        }
        total = sum(counts.values())
        parts = [f"{ex_id}={counts[ex_id]}" for ex_id in sorted(counts)]
        logger.info(
            "REST SNAPSHOT | start ex=%s total_symbols=%d interval=%ds",
            " ".join(parts) if parts else "-",
            total,
            interval_seconds,
        )

    @staticmethod
    def _format_rest_snapshot_summary(
        ok_counts: Dict[str, int],
        err_counts: Dict[str, int],
    ) -> str:
        all_exchanges = sorted(set(ok_counts) | set(err_counts))
        parts = []
        for ex_id in all_exchanges:
            ok = ok_counts.get(ex_id, 0)
            err = err_counts.get(ex_id, 0)
            parts.append(f"{ex_id}=ok:{ok} err:{err}")
        return " ".join(parts) if parts else "-"

    @staticmethod
    def _should_log_rest_snapshot(
        now: float, last_log: float, interval_seconds: int
    ) -> bool:
        if interval_seconds <= 0:
            return True
        return (now - last_log) >= interval_seconds

    def _log_market_status(self, markets: Dict[str, Dict]) -> None:
        """Log market load outcome for all configured exchanges.

        Args:
            markets: Dict mapping exchange_id → market data dict
        """
        for ex_id in settings.exchanges:
            m = markets.get(ex_id)
            if m is None:
                logger.error(
                    "MARKETS | %s missing (load_markets failed or exchange init failed)",
                    ex_id,
                )
            elif not m:
                logger.warning("MARKETS | %s empty markets", ex_id)
            else:
                logger.info("MARKETS | %s markets=%d", ex_id, len(m))

    async def _select_symbols(
        self, markets: Dict[str, Dict]
    ) -> tuple[Dict[str, List[str]], set[str]]:
        """Select symbols per exchange using configured strategy.

        Args:
            markets: Dict mapping exchange_id → market data

        Returns:
            Tuple of (symbols_per_exchange, all_symbols)
            - symbols_per_exchange: Dict[exchange_id, List[symbol]]
            - all_symbols: Set of all unique symbols across exchanges
        """
        exchange_symbol_limits = self._get_exchange_symbol_limits()

        # Cross-exchange aware symbol selection
        if settings.symbol_selection_strategy == "cross_exchange":
            valid_markets = {ex_id: m for ex_id, m in markets.items() if m}
            if len(valid_markets) < 2:
                logger.error(
                    "SYMBOLS | fewer than 2 exchanges with valid markets (%d), "
                    "falling back to local_volume",
                    len(valid_markets),
                )
                # Fall through to local_volume strategy
            else:
                # Build base index from markets
                symbol_index = build_symbol_index(valid_markets)

                # Enrich with volume data
                try:
                    volumes = await self._fetch_volume_all_with_retry()
                    merged = self._merge_volume_into_index(symbol_index, volumes)

                    if merged == 0:
                        logger.warning(
                            "VOLUME | No volume data merged, falling back to alphabetic ranking. "
                            "This may cause bias towards 'A*' symbols."
                        )
                except Exception as e:
                    logger.error(
                        "VOLUME | Fetch failed completely: %s. "
                        "Falling back to alphabetic ranking.",
                        e,
                    )

                # Apply minimum overlap and volume filters for reliability
                min_overlap = settings.symbol_min_overlap_exchanges
                if min_overlap > 2:
                    before = len(symbol_index)
                    symbol_index = {
                        sym: meta
                        for sym, meta in symbol_index.items()
                        if meta.exchange_count >= min_overlap
                    }
                    logger.info(
                        "SYMBOLS | overlap filter min_ex=%d kept=%d dropped=%d",
                        min_overlap,
                        len(symbol_index),
                        before - len(symbol_index),
                    )

                if settings.symbol_min_quote_volume_usd > 0 and merged > 0:
                    before = len(symbol_index)
                    symbol_index = {
                        sym: meta
                        for sym, meta in symbol_index.items()
                        if meta.aggregate_volume >= settings.symbol_min_quote_volume_usd
                    }
                    logger.info(
                        "SYMBOLS | volume filter min_usd=%.0f kept=%d dropped=%d",
                        settings.symbol_min_quote_volume_usd,
                        len(symbol_index),
                        before - len(symbol_index),
                    )
                elif settings.symbol_min_quote_volume_usd > 0 and merged == 0:
                    logger.warning(
                        "SYMBOLS | volume filter skipped (no volume data merged)"
                    )

                allowlist_symbols = None
                if settings.symbol_allowlist_path:
                    from parsertang.allowlist import load_allowlist

                    allowlist_symbols = load_allowlist(settings.symbol_allowlist_path)
                    if allowlist_symbols:
                        logger.info(
                            "SYMBOLS | allowlist path=%s symbols=%d",
                            settings.symbol_allowlist_path,
                            len(allowlist_symbols),
                        )
                if allowlist_symbols is None and settings.symbol_allowlist:
                    allowlist_symbols = list(settings.symbol_allowlist)

                if allowlist_symbols:
                    allow = {s.upper() for s in allowlist_symbols}
                    before = len(symbol_index)
                    symbol_index = {
                        sym: meta
                        for sym, meta in symbol_index.items()
                        if sym.upper() in allow
                    }
                    logger.info(
                        "SYMBOLS | allowlist applied kept=%d dropped=%d",
                        len(symbol_index),
                        before - len(symbol_index),
                    )

                # Rank and allocate
                ranked = rank_symbols_for_overlap(symbol_index)
                if settings.symbol_diversify_fraction > 0:
                    ranked = diversify_ranked_symbols(
                        ranked,
                        target_unique=settings.max_symbols_per_exchange,
                        diversify_fraction=settings.symbol_diversify_fraction,
                        pool_multiplier=settings.symbol_diversify_pool_multiplier,
                    )
                    logger.info(
                        "SYMBOLS | diversify fraction=%.2f pool_mult=%d",
                        settings.symbol_diversify_fraction,
                        settings.symbol_diversify_pool_multiplier,
                    )

                # Log metrics
                multi_ex_count = sum(
                    1 for m in symbol_index.values() if m.exchange_count >= 2
                )
                logger.info(
                    "SYMBOLS | strategy=cross_exchange total_candidates=%d multi_ex_symbols=%d",
                    len(symbol_index),
                    multi_ex_count,
                )

                symbols_per_exchange = allocate_symbols_per_exchange(
                    symbol_index,
                    ranked,
                    settings.max_symbols_per_exchange,
                    exchange_symbol_limits,
                    min_overlap_exchanges=settings.symbol_min_overlap_exchanges,
                )

                # Ensure stable keys for all configured exchanges
                for ex_id in settings.exchanges:
                    symbols_per_exchange.setdefault(ex_id, [])
                all_symbols = set()
                for syms in symbols_per_exchange.values():
                    all_symbols.update(syms)

                logger.info("Symbols discovered: %d unique", len(all_symbols))
                return symbols_per_exchange, all_symbols

        # Core+periphery selection strategy
        if settings.symbol_selection_strategy == "core_periphery":
            core = settings.core_exchanges or []
            periphery = settings.periphery_exchanges or []
            if not core:
                logger.error(
                    "SYMBOLS | core_periphery requires CORE_EXCHANGES, falling back to local_volume"
                )
            else:
                symbols_per_exchange = select_symbols_core_periphery(
                    markets,
                    settings.max_symbols_per_exchange,
                    core_exchanges=core,
                    periphery_exchanges=periphery,
                    exchange_limits=exchange_symbol_limits,
                )
                # Ensure stable keys for all configured exchanges
                for ex_id in settings.exchanges:
                    symbols_per_exchange.setdefault(ex_id, [])
                all_symbols = set()
                for syms in symbols_per_exchange.values():
                    all_symbols.update(syms)

                logger.info("Symbols discovered: %d unique", len(all_symbols))
                return symbols_per_exchange, all_symbols

        # Fallback: legacy local_volume strategy (per-exchange independent)
        symbols_per_exchange = {}
        all_symbols = set()

        for ex_id in settings.exchanges:
            m = markets.get(ex_id) or {}
            if not m:
                symbols_per_exchange[ex_id] = []
                continue

            # Filter: stable quote + spot markets + not excluded
            stable_symbols = self._filter_stable_symbols(m)
            allowlist_symbols = None
            if settings.symbol_allowlist_path:
                from parsertang.allowlist import load_allowlist

                allowlist_symbols = load_allowlist(settings.symbol_allowlist_path)
            if allowlist_symbols is None and settings.symbol_allowlist:
                allowlist_symbols = list(settings.symbol_allowlist)

            if allowlist_symbols:
                allow = {s.upper() for s in allowlist_symbols}
                stable_symbols = [s for s in stable_symbols if s.upper() in allow]

            # Sort by 24h volume (descending)
            symbols_with_volume = self._sort_by_volume(stable_symbols, m)

            # Apply per-exchange limit
            max_syms = exchange_symbol_limits.get(
                ex_id, settings.max_symbols_per_exchange
            )
            if max_syms and max_syms > 0 and len(symbols_with_volume) > max_syms:
                capped_syms = symbols_with_volume[:max_syms]
                logger.info(
                    "Loaded %d symbols from %s (limited to %d by MAX_SYMBOLS_PER_EXCHANGE)",
                    len(capped_syms),
                    ex_id,
                    max_syms,
                )
            else:
                capped_syms = symbols_with_volume
                logger.info("Loaded %d symbols from %s", len(capped_syms), ex_id)

            symbols_per_exchange[ex_id] = capped_syms
            all_symbols.update(capped_syms)

        logger.info("Symbols discovered: %d unique", len(all_symbols))
        return symbols_per_exchange, all_symbols

    async def _apply_symbol_selection(
        self, symbols_per_exchange: Dict[str, List[str]]
    ) -> None:
        """Apply new symbol selection to state and reset WS tracking."""
        assert self.state is not None
        self._ws_symbols_per_exchange = symbols_per_exchange
        self.state.ws_metrics.allocated_symbols = {
            ex_id: len(symbols_per_exchange.get(ex_id, []))
            for ex_id in settings.exchanges
        }
        self._log_symbol_allocation(symbols_per_exchange)
        await self._write_symbol_selection_snapshot(symbols_per_exchange)

        async with self.state.metrics_lock:
            self.state.ws_metrics.update_counters.clear()
            self.state.ws_metrics.symbols_seen.clear()
            self.state.ws_metrics.stale_intervals.clear()
            self.state.ws_metrics.last_logged = 0.0
            self.state.stats.clear()

        async with self.state.orderbooks_lock:
            self.state.orderbooks.clear()

    async def _write_symbol_selection_snapshot(
        self, symbols_per_exchange: Dict[str, List[str]]
    ) -> None:
        """Persist current WS symbol universe for ops/debugging."""
        path = Path("data/symbols_per_exchange.json")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)

            by_ex = {ex_id: list(symbols_per_exchange.get(ex_id, [])) for ex_id in settings.exchanges}
            sets = {ex_id: set(syms) for ex_id, syms in by_ex.items()}
            exchanges = [ex_id for ex_id in settings.exchanges if by_ex.get(ex_id)]

            pair_overlaps: dict[str, int] = {}
            for i, a in enumerate(exchanges):
                for b in exchanges[i + 1 :]:
                    pair_overlaps[f"{a}-{b}"] = len(sets[a] & sets[b])

            payload = {
                "ts": time.time(),
                "strategy": settings.symbol_selection_strategy,
                "min_overlap_exchanges": settings.symbol_min_overlap_exchanges,
                "min_quote_volume_usd": settings.symbol_min_quote_volume_usd,
                "max_symbols_per_exchange": settings.max_symbols_per_exchange,
                "exchange_limits": get_exchange_symbol_limits(),
                "counts": {ex_id: len(by_ex.get(ex_id, [])) for ex_id in settings.exchanges},
                "pair_overlaps": pair_overlaps,
                "symbols": by_ex,
            }
            text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
            await asyncio.to_thread(path.write_text, text, encoding="utf-8")
            logger.info(
                "SYMBOLS SNAPSHOT | wrote %s exchanges=%d overlaps=%s",
                str(path),
                len(exchanges),
                ",".join(f"{k}:{v}" for k, v in sorted(pair_overlaps.items())) or "-",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("SYMBOLS SNAPSHOT | write failed path=%s err=%s", path, exc)

    def _select_rest_snapshot_symbols(
        self,
        markets: Dict[str, Dict],
        ws_symbols_per_exchange: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        if not settings.rest_snapshot_enabled:
            return {}

        assert self.gateway is not None

        selected: Dict[str, List[str]] = {}
        for ex_id in settings.rest_snapshot_exchanges:
            ex_markets = markets.get(ex_id) or {}
            if not ex_markets:
                selected[ex_id] = []
                continue

            stable_symbols = self._filter_stable_symbols(ex_markets)
            ranked = self._sort_by_volume(stable_symbols, ex_markets)
            ws_symbols = set(ws_symbols_per_exchange.get(ex_id, []))
            rest_symbols = [s for s in ranked if s not in ws_symbols]
            if settings.rest_snapshot_max_symbols > 0:
                rest_symbols = rest_symbols[: settings.rest_snapshot_max_symbols]
            selected[ex_id] = rest_symbols

        return selected

    async def _clear_exchange_state(self, ex_id: str) -> None:
        assert self.state is not None

        async with self.state.orderbooks_lock:
            keys = [k for k in self.state.orderbooks.keys() if k[0] == ex_id]
            for key in keys:
                del self.state.orderbooks[key]

        async with self.state.metrics_lock:
            self.state.ws_metrics.update_counters.pop(ex_id, None)
            self.state.ws_metrics.symbols_seen.pop(ex_id, None)
            self.state.ws_metrics.stale_intervals.pop(ex_id, None)

    async def _refresh_symbols(self) -> tuple[Dict[str, List[str]], Dict[str, Dict]]:
        """Reload markets and recompute symbol selection."""
        assert self.gateway is not None
        markets = await asyncio.to_thread(self.gateway.load_markets)
        self._log_market_status(markets)
        symbols_per_exchange, _ = await self._select_symbols(markets)
        await self._apply_symbol_selection(symbols_per_exchange)
        self._symbol_refresh_last_ts = time.time()
        return symbols_per_exchange, markets

    async def _symbol_refresh_watcher(self) -> str:
        """Wait until adaptive refresh conditions are met, then return reason."""
        assert self.state is not None
        last_arb_ok = 0
        last_arb_reject = 0

        while True:
            check_interval = settings.symbol_refresh_check_interval_seconds
            if self._symbol_ramp_limiter:
                check_interval = min(
                    check_interval, settings.symbol_ramp_check_interval_seconds
                )
            await asyncio.sleep(check_interval)

            allowlist_path = settings.symbol_allowlist_path
            if allowlist_path and settings.symbol_allowlist_refresh_seconds > 0:
                try:
                    mtime = os.stat(allowlist_path).st_mtime
                except FileNotFoundError:
                    mtime = 0.0
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "SYMBOLS | allowlist stat failed path=%s err=%s",
                        allowlist_path,
                        exc,
                    )
                    mtime = 0.0

                now_ts = time.time()
                if (
                    mtime > 0
                    and mtime > self._allowlist_last_mtime
                    and (
                        now_ts - self._allowlist_refresh_last_ts
                        >= settings.symbol_allowlist_refresh_seconds
                    )
                ):
                    self._allowlist_last_mtime = mtime
                    self._allowlist_refresh_last_ts = now_ts
                    logger.info(
                        "SYMBOLS | allowlist changed path=%s mtime=%.0f -> refresh",
                        allowlist_path,
                        mtime,
                    )
                    return "allowlist_update"

            async with self.state.metrics_lock:
                arb_ok = self.state.funnel_counters.get("arb_ok", 0)
                arb_reject = self.state.funnel_counters.get("arb_reject_threshold", 0)
                stale_intervals = dict(self.state.ws_metrics.stale_intervals)
                allocated_symbols = dict(self.state.ws_metrics.allocated_symbols)

            arb_ok_delta = arb_ok - last_arb_ok
            arb_reject_delta = arb_reject - last_arb_reject
            last_arb_ok = arb_ok
            last_arb_reject = arb_reject

            stale_exchanges = sum(
                1
                for v in stale_intervals.values()
                if v >= settings.symbol_refresh_stale_intervals_threshold
            )

            if self._symbol_ramp_limiter:
                multi_ex_symbols = await self._compute_multi_ex_symbols()
                ramp_stale_exchanges = sum(
                    1
                    for ex_id, stale in stale_intervals.items()
                    if allocated_symbols.get(ex_id, 0) > 0
                    and stale >= settings.symbol_refresh_stale_intervals_threshold
                )
                snapshot = HealthSnapshot(
                    now_ts=time.time(),
                    multi_ex_symbols=multi_ex_symbols,
                    stale_exchanges=ramp_stale_exchanges,
                )
                decision = self._symbol_ramp_limiter.evaluate(snapshot)
                if decision:
                    if (
                        snapshot.now_ts - self._symbol_refresh_last_ts
                        < settings.symbol_ramp_min_interval_seconds
                    ):
                        logger.info(
                            "SYMBOL RAMP | action=%s deferred (min_interval)",
                            decision.action,
                        )
                    else:
                        logger.info(
                            "SYMBOL RAMP | action=%s reason=%s multi_ex=%d stale=%d limits=%s",
                            decision.action,
                            decision.reason,
                            snapshot.multi_ex_symbols,
                            snapshot.stale_exchanges,
                            decision.limits,
                        )
                        return f"symbol_ramp_{decision.action}"

            inputs = RefreshInputs(
                now_ts=time.time(),
                last_refresh_ts=self._symbol_refresh_last_ts,
                min_interval_seconds=settings.symbol_refresh_min_interval_seconds,
                arb_ok_delta=arb_ok_delta,
                arb_reject_delta=arb_reject_delta,
                stale_exchanges=stale_exchanges,
                stale_exchanges_threshold=(
                    settings.symbol_refresh_stale_exchanges_threshold
                ),
                min_arb_ok=settings.symbol_refresh_min_arb_ok,
                min_arb_reject=settings.symbol_refresh_min_arb_reject,
            )
            should_refresh, reason = should_refresh_symbols(inputs)
            if should_refresh:
                logger.info(
                    "SYMBOL REFRESH | trigger=%s ok=%d reject=%d stale_ex=%d",
                    reason,
                    arb_ok_delta,
                    arb_reject_delta,
                    stale_exchanges,
                )
                return reason

    def _get_exchange_symbol_limits(self) -> dict[str, int]:
        if self._symbol_ramp_limiter:
            return dict(self._symbol_ramp_limiter.current_limits)
        return get_exchange_symbol_limits()

    def _build_symbol_ramp_limiter(self) -> AdaptiveSymbolLimiter | None:
        if not settings.symbol_ramp_enabled:
            return None

        base_limits = dict(get_exchange_symbol_limits())
        for ex_id in settings.exchanges:
            base_limits.setdefault(ex_id, settings.max_symbols_per_exchange)

        max_limits = dict(base_limits)
        if settings.symbol_ramp_max_limits_json:
            max_limits.update(
                self._parse_limits_json(settings.symbol_ramp_max_limits_json)
            )
        elif settings.symbol_ramp_max_increase > 0:
            for ex_id, limit in base_limits.items():
                max_limits[ex_id] = limit + settings.symbol_ramp_max_increase

        for ex_id, limit in base_limits.items():
            max_limits[ex_id] = max(max_limits.get(ex_id, limit), limit)

        limiter = AdaptiveSymbolLimiter(
            base_limits=base_limits,
            max_limits=max_limits,
            core_exchanges=settings.core_exchanges or [],
            periphery_exchanges=settings.periphery_exchanges or [],
            step_core=settings.symbol_ramp_step_core,
            step_periphery=settings.symbol_ramp_step_periphery,
            window_seconds=settings.symbol_ramp_window_seconds,
            min_multi_ex_symbols=settings.symbol_ramp_min_multi_ex_symbols,
            max_stale_exchanges=settings.symbol_ramp_max_stale_exchanges,
        )
        logger.info(
            "SYMBOL RAMP | enabled window=%ds step_core=%d step_periphery=%d min_multi_ex=%d max_stale=%d max_increase=%d",
            settings.symbol_ramp_window_seconds,
            settings.symbol_ramp_step_core,
            settings.symbol_ramp_step_periphery,
            settings.symbol_ramp_min_multi_ex_symbols,
            settings.symbol_ramp_max_stale_exchanges,
            settings.symbol_ramp_max_increase,
        )
        return limiter

    def _parse_limits_json(self, raw: str) -> dict[str, int]:
        try:
            parsed = json.loads(raw)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Invalid SYMBOL_RAMP_MAX_LIMITS_JSON: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("SYMBOL_RAMP_MAX_LIMITS_JSON must be a JSON object")
        limits: dict[str, int] = {}
        for ex_id, value in parsed.items():
            if not isinstance(ex_id, str):
                raise ValueError("SYMBOL_RAMP_MAX_LIMITS_JSON keys must be strings")
            try:
                limit_int = int(value)
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    f"SYMBOL_RAMP_MAX_LIMITS_JSON[{ex_id!r}] must be int"
                ) from e
            if limit_int < 0:
                raise ValueError(f"SYMBOL_RAMP_MAX_LIMITS_JSON[{ex_id!r}] must be >= 0")
            if limit_int > 5000:
                raise ValueError(
                    f"SYMBOL_RAMP_MAX_LIMITS_JSON[{ex_id!r}] too large (>5000)"
                )
            limits[ex_id] = limit_int
        return limits

    async def _compute_multi_ex_symbols(self) -> int:
        assert self.state is not None
        symbol_exchange_count: dict[str, set[str]] = {}
        async with self.state.orderbooks_lock:
            for ex_id, symbol in self.state.orderbooks.keys():
                symbol_exchange_count.setdefault(symbol, set()).add(ex_id)
        return sum(1 for exs in symbol_exchange_count.values() if len(exs) >= 2)

    def _filter_stable_symbols(self, markets: Dict) -> List[str]:
        """Filter markets for stable quote currency and spot type.

        Args:
            markets: Market data dict from exchange

        Returns:
            List of symbol strings that pass filters
        """
        stable_symbols = []
        assert self.gateway is not None

        for s in markets.keys():
            # Must have stable quote (USDT, USDC, DAI, etc.)
            if not self.gateway.is_stable_quote(s):
                continue

            market_info = markets.get(s, {})
            if isinstance(market_info, dict):
                if market_info.get("active") is False:
                    continue
                # Skip futures/swaps
                if market_info.get("spot") is True:
                    pass
                elif (
                    market_info.get("swap") is True or market_info.get("future") is True
                ):
                    continue
                else:
                    # Skip derivatives with ':' in symbol
                    if ":" in s:
                        continue
            else:
                if ":" in s:
                    continue

            # Skip excluded base assets
            try:
                base, _quote = s.split("/")
                if base.upper() in EXCLUDED_BASE_ASSETS:
                    continue
            except ValueError:
                continue

            stable_symbols.append(s)

        return stable_symbols

    def _sort_by_volume(self, symbols: List[str], markets: Dict) -> List[str]:
        """Sort symbols by 24h quote volume (descending).

        Args:
            symbols: List of symbols to sort
            markets: Market data dict

        Returns:
            Sorted list of symbols (highest volume first)
        """
        symbols_with_volume = []
        for sym in symbols:
            market_info = markets.get(sym, {})
            volume = 0.0
            if isinstance(market_info, dict):
                info = market_info.get("info", {})
                if isinstance(info, dict):
                    volume = float(
                        info.get("quoteVolume", 0) or info.get("volume", 0) or 0
                    )
            symbols_with_volume.append((sym, volume))

        # Sort by volume desc, then symbol name asc
        symbols_with_volume.sort(key=lambda x: (-x[1], x[0]))
        return [sym for sym, _ in symbols_with_volume]

    async def _fetch_volume_for_exchange(self, exchange_id: str) -> dict[str, float]:
        """Fetch 24h quote volume for spot USDT pairs.

        Returns:
            Dict of symbol → quoteVolume (USD). Empty dict on failure.
        """
        assert self.gateway is not None

        client = self.gateway.exchanges.get(exchange_id)
        if not client or not client.markets:
            return {}

        # Filter spot USDT symbols
        spot_symbols = [
            s
            for s, m in client.markets.items()
            if m.get("spot") and "/USDT" in s and ":" not in s
        ]

        if not spot_symbols:
            return {}

        # Limit to avoid rate limits (most exchanges handle 200+ fine)
        spot_symbols = spot_symbols[:500]

        # Run sync ccxt in thread to avoid blocking event loop
        tickers = await asyncio.to_thread(client.fetch_tickers, spot_symbols)

        result: dict[str, float] = {}
        for sym, ticker in tickers.items():  # type: ignore[union-attr]
            result[sym] = float(ticker.get("quoteVolume", 0) or 0)
        return result

    async def _fetch_volume_all_with_retry(
        self, max_retries: int = 3
    ) -> dict[str, dict[str, float]]:
        """Fetch volume from all exchanges with retry logic.

        Returns:
            Dict of exchange_id → {symbol: volume}. Partial results on failures.
        """
        results: dict[str, dict[str, float]] = {}

        for ex_id in settings.exchanges:
            for attempt in range(max_retries):
                try:
                    vol_data = await self._fetch_volume_for_exchange(ex_id)
                    results[ex_id] = vol_data

                    if vol_data:
                        logger.info(
                            "VOLUME | %s fetched %d symbols", ex_id, len(vol_data)
                        )
                    else:
                        logger.warning("VOLUME | %s returned empty data", ex_id)
                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        wait = 2**attempt  # 1s, 2s, 4s
                        logger.warning(
                            "VOLUME | %s attempt %d failed: %s, retry in %ds",
                            ex_id,
                            attempt + 1,
                            e,
                            wait,
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(
                            "VOLUME | %s failed after %d attempts: %s",
                            ex_id,
                            max_retries,
                            e,
                        )
                        results[ex_id] = {}

        return results

    def _merge_volume_into_index(
        self,
        symbol_index: dict[str, SymbolMeta],
        volumes: dict[str, dict[str, float]],
    ) -> int:
        """Merge fetched volume data into symbol index.

        Updates SymbolMeta.exchanges dict with volume values.

        Returns:
            Number of symbols updated with volume data.
        """
        updated = 0

        for ex_id, vol_data in volumes.items():
            for symbol, volume in vol_data.items():
                if symbol in symbol_index and volume > 0:
                    symbol_index[symbol].exchanges[ex_id] = volume
                    updated += 1

        logger.info("VOLUME | merged %d symbol-exchange pairs", updated)
        return updated

    def _log_symbol_allocation(
        self, symbols_per_exchange: Dict[str, List[str]]
    ) -> None:
        """Log final symbol allocation per exchange.

        Args:
            symbols_per_exchange: Dict mapping exchange_id → list of symbols
        """
        exchange_symbol_limits = get_exchange_symbol_limits()

        for ex_id in settings.exchanges:
            max_syms = exchange_symbol_limits.get(
                ex_id, settings.max_symbols_per_exchange
            )
            count = len(symbols_per_exchange.get(ex_id, []))
            logger.info(
                "SYMBOLS | %s allocated %d symbols (max=%d)", ex_id, count, max_syms
            )

        # Log sample symbols for debugging
        gate_syms = symbols_per_exchange.get("gate", [])
        preview = ",".join(gate_syms[:10]) if gate_syms else "-"
        logger.info(
            "SYMBOLS DETAIL | gate symbols=%s count=%d",
            preview,
            len(gate_syms),
        )

    async def _start_metadata_refresh(self) -> None:
        """Start metadata refresh background task (fees, currencies).

        Refreshes immediately, then periodically every 60 seconds.
        """
        assert self.gateway is not None
        assert self.state is not None

        # Initial refresh (fire-and-forget)
        self._create_background_task(
            self._refresh_metadata_once(),
            name="refresh_metadata_once",
        )

        # Periodic refresh task
        metadata_task = self._create_background_task(
            self._metadata_refresher(60),
            name="metadata_refresher",
        )
        self.background_tasks.append(metadata_task)

    async def _refresh_metadata_once(self) -> None:
        """Refresh exchange metadata once (fees and currencies).

        Called on startup and periodically by metadata_refresher.
        Stores results in AppState.currency_cache and fee_cache.
        """
        assert self.gateway is not None
        assert self.state is not None

        for ex_id in settings.exchanges:
            try:
                fees_raw = self.gateway.fetch_fees(ex_id) or {}
                curr_raw = self.gateway.fetch_currencies(ex_id) or {}

                # Normalize fee percentages
                taker_raw = float(fees_raw.get("taker", 0.0) or 0.0)
                maker_raw = float(fees_raw.get("maker", 0.0) or 0.0)
                taker_pct = taker_raw * 100.0 if taker_raw <= 1 else taker_raw
                maker_pct = maker_raw * 100.0 if maker_raw <= 1 else maker_raw

                # Update state caches
                await self.state.update_fee_cache(ex_id, taker_pct, maker_pct)
                await self.state.update_currency_cache(ex_id, curr_raw)

                logger.info(
                    "META REFRESH | %s fees: taker=%.2f%% maker=%.2f%%",
                    ex_id,
                    taker_pct,
                    maker_pct,
                )

                # Log network fees for stablecoins (diagnostic)
                for stable in ("USDT", "USDC", "DAI"):
                    currency_info = curr_raw.get(stable)
                    if not currency_info:
                        continue
                    networks = currency_info.get("networks") or {}
                    if not networks:
                        continue
                    parts = []
                    for net, data in networks.items():
                        fee_val = data.get("fee")
                        if fee_val is None:
                            continue
                        try:
                            parts.append(f"{net}({float(fee_val)})")
                        except (TypeError, ValueError):
                            continue
                    if parts:
                        logger.info(
                            "NETWORKS | %s %s: %s", ex_id, stable, ", ".join(parts)
                        )

            except Exception as err:  # noqa: BLE001
                logger.warning("META ERROR | %s %s", ex_id, err)

    async def _metadata_refresher(self, interval_seconds: int) -> None:
        """Periodic metadata refresh task.

        Args:
            interval_seconds: Seconds between refresh cycles
        """
        while True:
            await asyncio.sleep(interval_seconds)
            await self._refresh_metadata_once()

    async def _initialize_fees(self, proxy_config: Dict | None) -> None:
        """Initialize withdrawal fee manager with authenticated exchanges.

        Args:
            proxy_config: Optional proxy configuration dict
        """
        assert self.state is not None

        if not settings.use_dynamic_withdrawal_fees:
            logger.info(
                "FEES | Dynamic withdrawal fees disabled (USE_DYNAMIC_WITHDRAWAL_FEES=false)"
            )
            return

        try:
            import ccxt.pro
            from parsertang.utils.exchange_credentials import build_exchange_config

            # Create authenticated exchanges for fee fetching
            authenticated_exchanges = {}
            for ex_id in settings.exchanges:
                try:
                    # Get exchange class (handle WS aliases like htx → huobi)
                    class_name = WS_ID_ALIASES.get(ex_id, ex_id)
                    if not hasattr(ccxt.pro, class_name):
                        logger.warning(
                            f"FEES | Exchange {ex_id} not supported by ccxt.pro"
                        )
                        continue

                    cls = getattr(ccxt.pro, class_name)
                    config = build_exchange_config(ex_id, settings, proxy_config)
                    exchange = cls(config)
                    authenticated_exchanges[ex_id] = exchange

                    # Log authentication status
                    if "apiKey" in config:
                        logger.info(f"FEES | {ex_id} initialized with API credentials")
                    else:
                        logger.warning(
                            f"FEES | {ex_id} initialized WITHOUT credentials (may return empty data)"
                        )

                except Exception as e:
                    logger.error(f"FEES ERROR | Failed to initialize {ex_id}: {e}")

            if not authenticated_exchanges:
                logger.error("FEES ERROR | No exchanges initialized for fee manager")
                return

            # Create fee manager
            fee_manager = WithdrawalFeeManager(
                exchanges=authenticated_exchanges,
                cache_lifetime=settings.withdrawal_fee_cache_lifetime,
                fetch_timeout=settings.withdrawal_fee_fetch_timeout,
            )

            # Start background refresh (includes initial fetch)
            await fee_manager.start_background_refresh()

            # Update state
            self.state.fee_manager = fee_manager

            logger.info(
                f"FEES | Withdrawal fee manager initialized with {len(authenticated_exchanges)} exchanges"
            )

        except Exception as e:
            logger.error(
                f"FEES ERROR | Failed to initialize withdrawal fee manager: {e}"
            )

    async def _initialize_trader(self) -> None:
        """Initialize trader (if dry-run or trading enabled)."""
        assert self.state is not None

        if settings.dry_run_mode or settings.trading_enabled:
            self.state.trader = SimpleTrader(
                dry_run=settings.dry_run_mode,
                max_concurrent_cycles=settings.max_concurrent_cycles,
            )
            logger.info(
                "Trader initialized: dry_run=%s, max_concurrent_cycles=%d",
                settings.dry_run_mode,
                settings.max_concurrent_cycles,
            )
        else:
            logger.info("Trader disabled (DRY_RUN_MODE=false, TRADING_ENABLED=false)")

    async def _initialize_bot(self) -> None:
        """Initialize Telegram bot for commands (if credentials configured)."""
        assert self.state is not None
        assert self.gateway is not None

        if settings.telegram_bot_token and settings.telegram_chat_id:
            try:
                bot = SimpleBot(trader=self.state.trader, gateway=self.gateway)
                await bot.start()
                self.state.bot = bot
                logger.info("SimpleBot started (async mode) for Telegram commands")
            except Exception as e:
                logger.warning("Failed to start SimpleBot: %s", e)
        else:
            logger.info(
                "SimpleBot disabled (no TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)"
            )

    def _start_background_tasks(self) -> None:
        """Start all background monitoring tasks.

        Tasks:
        - Spread distribution logging (5min interval)
        - WebSocket health logging (60s interval)
        - Stale data cleanup (5min interval)
        - Trader statistics (30s interval, if trader enabled)
        """
        assert self.state is not None

        # Spread distribution
        self.background_tasks.append(
            self._create_background_task(
                log_spread_distribution(self.state),
                name="log_spread_distribution",
            )
        )

        # V2 validation summary
        self.background_tasks.append(
            self._create_background_task(
                log_validation_summary(self.state),
                name="log_validation_summary",
            )
        )

        # Fee validation summary
        self.background_tasks.append(
            self._create_background_task(
                log_fee_validation_summary(self.state),
                name="log_fee_validation_summary",
            )
        )

        # Alert funnel summary (candidate → suppressed → sent)
        self.background_tasks.append(
            self._create_background_task(
                log_alert_funnel_summary(self.state),
                name="log_alert_funnel_summary",
            )
        )

        # WebSocket health
        self.background_tasks.append(
            self._create_background_task(
                log_ws_health(self.state, self._resolve_ws_exchanges()),
                name="log_ws_health",
            )
        )

        # Cleanup stale data
        self.background_tasks.append(
            self._create_background_task(
                cleanup_stale_data(self.state),
                name="cleanup_stale_data",
            )
        )

        # Trader stats (if trader enabled)
        if self.state.trader:
            self.background_tasks.append(
                self._create_background_task(
                    log_trader_stats(self.state),
                    name="log_trader_stats",
                )
            )

        # Daily fee report (if enabled)
        if settings.enable_daily_fee_report and self.state.fee_stats:
            if self.state.alert_service:
                self.background_tasks.append(
                    self._create_background_task(
                        daily_fee_report_task(
                            self.state.fee_stats,
                            self.state.alert_service.send_tech,
                        ),
                        name="daily_fee_report_task",
                    )
                )
                logger.info(
                    "Daily fee report task started (tech_chat_id=%s)",
                    settings.telegram_tech_chat_id,
                )
            else:
                logger.warning(
                    "Daily fee report enabled but no alert_service configured"
                )

        if settings.ws_guard_enabled:
            self.background_tasks.append(
                self._create_background_task(
                    self._ws_guard_task(),
                    name="ws_guard",
                )
            )

    def _create_background_task(self, coro: Awaitable[None], name: str) -> asyncio.Task:
        """Create background task with error logging.

        Args:
            coro: Coroutine to run as background task
            name: Task name (for logging)

        Returns:
            Created asyncio.Task
        """

        async def safe_background_task() -> None:
            try:
                await coro
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error("Background task '%s' failed: %s", name, e, exc_info=True)

        return asyncio.create_task(safe_background_task())

    async def _ws_guard_task(self) -> None:
        assert self.state is not None

        guard = WSGuard(
            no_overlap_minutes=settings.ws_guard_no_overlap_minutes,
            restart_min_interval_minutes=settings.ws_guard_restart_min_interval_minutes,
            state_path=Path(settings.ws_guard_state_path),
            log_path=Path(settings.ws_guard_log_path),
            min_active_exchanges=settings.ws_guard_min_active_exchanges,
            stale_exchanges_threshold=settings.ws_guard_stale_exchanges_threshold,
            check_interval_seconds=settings.ws_guard_check_interval_seconds,
        )

        def exit_fn() -> None:
            logger.error("WS GUARD | triggering restart")
            os._exit(1)

        async def recover_fn(stale_exchanges: list[str] | None = None) -> bool:
            return await self.request_ws_recover(exchanges=stale_exchanges)

        while True:
            await asyncio.sleep(settings.ws_guard_check_interval_seconds)
            await guard_once(
                self.state,
                self.state.alert_service,
                guard,
                exit_fn=exit_fn,
                recover_fn=recover_fn,
                snapshot_fn=build_incident_snapshot,
            )

    def _start_refresh_trace(self, reason: str) -> None:
        start_ts = time.time()
        self._refresh_trace = RefreshTrace(reason=reason, start_ts=start_ts)
        self._refresh_trace.mark("start", start_ts)
        self._refresh_trace_reported = False
        if self._refresh_trace_timeout_task:
            self._refresh_trace_timeout_task.cancel()
            self._refresh_trace_timeout_task = None
        logger.info("REFRESH TRACE | start reason=%s", reason)

    def _mark_refresh_trace(self, step: str) -> None:
        if not self._refresh_trace:
            return
        self._refresh_trace.mark(step, time.time())
        logger.info("REFRESH TRACE | step=%s", step)

    def _cancel_refresh_trace_timeout(self) -> None:
        if self._refresh_trace_timeout_task:
            self._refresh_trace_timeout_task.cancel()
            self._refresh_trace_timeout_task = None

    def _report_refresh_trace(
        self, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        if (
            not self._refresh_trace
            or self._refresh_trace_reported
            or not self._refresh_trace.has_start()
        ):
            return
        summary = self._refresh_trace.summary()
        logger.info(summary)
        if self.state and self.state.alert_service:
            if loop is None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
            if loop:
                loop.create_task(self.state.alert_service.send_tech(summary))
        self._refresh_trace_reported = True

    async def _refresh_trace_timeout(self) -> None:
        await asyncio.sleep(REFRESH_TRACE_TIMEOUT_SECONDS)
        self._report_refresh_trace()

    async def restart_exchange(
        self,
        ex_id: str,
        timeout_seconds: float | None = None,
        reason: str | None = None,
    ) -> bool:
        if not self.streams or not self.state:
            return False

        start_ts = time.time()
        logger.info("L0 RECOVER | start ex=%s reason=%s", ex_id, reason or "unknown")

        now = time.time()
        last_restart = self._ws_last_restart.get(ex_id)
        if (
            last_restart is not None
            and now - last_restart < self._ws_l0_cooldown_seconds
        ):
            logger.warning(
                "L0 RECOVER | %s cooldown active (last_restart=%.0fs ago)",
                ex_id,
                now - last_restart,
            )
            return False

        task = self._ws_tasks.pop(ex_id, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        client = self._ws_clients.pop(ex_id, None)
        if client:
            try:
                closer = getattr(client, "close", None)
                if closer:
                    result = closer()
                    if asyncio.iscoroutine(result):
                        await result
            except Exception as e:
                logger.warning("L0 RECOVER | %s close failed: %s", ex_id, e)

        await self._clear_exchange_state(ex_id)

        proxy_config = self._build_proxy_config()
        client = self.streams.create_exchange(ex_id, proxy_config=proxy_config or None)
        if client is None:
            logger.warning("L0 RECOVER | %s client init failed", ex_id)
            return False

        self._ws_clients[ex_id] = client
        self._ws_last_restart[ex_id] = now

        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()
        self._ws_exchange_recover_futures[ex_id] = future

        base_on_update = self._ws_on_update or (lambda *_args, **_kwargs: None)

        def on_update(ex_id_update: str, symbol: str, ob: dict) -> None:
            f = self._ws_exchange_recover_futures.get(ex_id)
            if f and not f.done():
                f.set_result(True)
            base_on_update(ex_id_update, symbol, ob)

        symbols = self._ws_symbols_per_exchange.get(ex_id, [])
        self._ws_tasks[ex_id] = self.streams.start_exchange_worker(
            ex_id, client, symbols, on_update
        )

        timeout = timeout_seconds or self._ws_l0_timeout_seconds
        try:
            ok = await asyncio.wait_for(asyncio.shield(future), timeout)
            if ok:
                elapsed = time.time() - start_ts
                logger.info("L0 RECOVER | first_update ex=%s dt=%.1fs", ex_id, elapsed)
            return bool(ok)
        except asyncio.TimeoutError:
            elapsed = time.time() - start_ts
            logger.warning("L0 RECOVER | timeout ex=%s dt=%.1fs", ex_id, elapsed)
            return False
        finally:
            self._ws_exchange_recover_futures.pop(ex_id, None)

    async def request_ws_recover(
        self,
        timeout_seconds: int | None = None,
        exchanges: list[str] | None = None,
        expected_exchanges: list[str] | None = None,
    ) -> bool:
        if exchanges:
            for ex_id in exchanges:
                ok = await self.restart_exchange(ex_id, reason="stale_exchanges")
                if not ok:
                    return await self.request_ws_recover(
                        timeout_seconds=timeout_seconds,
                        exchanges=None,
                        expected_exchanges=exchanges,
                    )
            return True

        if self._ws_recover_future and not self._ws_recover_future.done():
            timeout = timeout_seconds or self._ws_recover_timeout_seconds
            try:
                return await asyncio.wait_for(
                    asyncio.shield(self._ws_recover_future), timeout
                )
            except asyncio.TimeoutError:
                return False

        loop = asyncio.get_running_loop()
        self._ws_recover_future = loop.create_future()
        self._ws_recover_event.set()
        self._ws_recover_expected_exchanges = (
            set(expected_exchanges) if expected_exchanges else None
        )
        timeout = timeout_seconds or self._ws_recover_timeout_seconds
        try:
            return await asyncio.wait_for(
                asyncio.shield(self._ws_recover_future), timeout
            )
        except asyncio.TimeoutError:
            return False

    def _cancel_ws_recover_timeout(self) -> None:
        if self._ws_recover_timeout_task:
            self._ws_recover_timeout_task.cancel()
            self._ws_recover_timeout_task = None

    def _create_ws_gather_task(self, tasks: list[asyncio.Task]) -> asyncio.Task:
        async def wait_all() -> None:
            await asyncio.gather(*tasks)

        return asyncio.create_task(wait_all())

    async def _cancel_ws_tasks(self) -> None:
        if not self._ws_tasks:
            return
        tasks = list(self._ws_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._ws_tasks.clear()

    def _schedule_ws_recover_timeout(self) -> None:
        self._cancel_ws_recover_timeout()
        self._ws_recover_timeout_task = asyncio.create_task(self._ws_recover_timeout())

    async def _ws_recover_timeout(self) -> None:
        await asyncio.sleep(self._ws_recover_timeout_seconds)
        if self._ws_recover_future and not self._ws_recover_future.done():
            self._ws_recover_future.set_result(False)
            logger.warning("L1 RECOVER | timeout waiting for first update")
            if self._ws_recover_expected_exchanges is not None:
                self._ws_recover_expected_exchanges = set()

    def _create_orderbook_callback(
        self, loop: asyncio.AbstractEventLoop
    ) -> Callable[[str, str, dict], None]:
        """Create orderbook update callback with all dependencies injected.

        Args:
            loop: Event loop for creating async tasks

        Returns:
            Callback function compatible with Streams.subscribe_orderbooks
        """
        assert self.state is not None

        def callback(ex_id: str, symbol: str, ob: dict) -> None:
            """Wrapper callback that injects all dependencies."""
            assert self.state is not None
            if self._refresh_trace and self._refresh_trace.first_update is None:
                self._refresh_trace.mark_first_update(ex_id, symbol, time.time())
                self._cancel_refresh_trace_timeout()
                self._report_refresh_trace(loop)
            if self._ws_recover_future and not self._ws_recover_future.done():
                expected = self._ws_recover_expected_exchanges
                if expected is None or ex_id in expected:
                    self._ws_recover_future.set_result(True)
                    self._cancel_ws_recover_timeout()
                    if expected is not None:
                        self._ws_recover_expected_exchanges = set()
                    logger.info(
                        "L1 RECOVER | first_update ex=%s symbol=%s",
                        ex_id,
                        symbol,
                    )

            # Create partial evaluation function that captures state and loop
            def evaluate_fn(sym: str) -> None:
                assert self.state is not None
                evaluate_arbitrage_for_symbol(sym, self.state, loop)

            # Call processor with all dependencies
            on_orderbook_update(
                ex_id=ex_id,
                symbol=symbol,
                ob=ob,
                state=self.state,
                settings={
                    "liquidity_window_pct": settings.liquidity_window_pct,
                    "liquidity_usd_threshold": settings.liquidity_usd_threshold,
                    "trade_volume_usd": settings.trade_volume_usd,
                },
                liquidity_fn=liquidity_usd_within_window,
                slippage_buy_fn=estimate_buy_slippage_pct,
                slippage_sell_fn=estimate_sell_slippage_pct,
                evaluate_arbitrage_fn=evaluate_fn,
            )

            # Shadow V2 pipeline (no side effects). Errors are swallowed to not affect prod path.
            if self._shadow_pipeline:
                try:
                    self._shadow_pipeline.on_orderbook(ex_id, symbol, ob)
                except Exception:
                    logger.debug(
                        "V2 SHADOW ERROR | %s %s", ex_id, symbol, exc_info=True
                    )

        return callback

    def _create_rest_orderbook_callback(
        self, loop: asyncio.AbstractEventLoop
    ) -> Callable[[str, str, dict], None]:
        assert self.state is not None

        def callback(ex_id: str, symbol: str, ob: dict) -> None:
            assert self.state is not None

            def evaluate_fn(sym: str) -> None:
                assert self.state is not None
                evaluate_arbitrage_for_symbol(sym, self.state, loop)

            on_orderbook_update(
                ex_id=ex_id,
                symbol=symbol,
                ob=ob,
                state=self.state,
                settings={
                    "liquidity_window_pct": settings.liquidity_window_pct,
                    "liquidity_usd_threshold": settings.liquidity_usd_threshold,
                    "trade_volume_usd": settings.trade_volume_usd,
                },
                liquidity_fn=liquidity_usd_within_window,
                slippage_buy_fn=estimate_buy_slippage_pct,
                slippage_sell_fn=estimate_sell_slippage_pct,
                evaluate_arbitrage_fn=evaluate_fn,
                update_ws_metrics=False,
            )

        return callback

    async def _rest_snapshot_loop(
        self,
        symbols_per_exchange: Dict[str, List[str]],
        callback: Callable[[str, str, dict], None],
    ) -> None:
        assert self.gateway is not None
        self._log_rest_snapshot_start(
            symbols_per_exchange,
            interval_seconds=settings.rest_snapshot_interval_seconds,
        )
        ok_counts: dict[str, int] = {}
        err_counts: dict[str, int] = {}
        last_log = time.time()
        while True:
            start = time.time()
            for ex_id, symbols in symbols_per_exchange.items():
                for sym in symbols:
                    try:
                        bids, asks = await asyncio.to_thread(
                            self.gateway.fetch_order_book,
                            ex_id,
                            sym,
                            settings.orderbook_limit,
                        )
                        callback(ex_id, sym, {"bids": bids, "asks": asks})
                        ok_counts[ex_id] = ok_counts.get(ex_id, 0) + 1
                    except CircuitOpenError as e:
                        logger.warning("REST SNAPSHOT | %s circuit open: %s", ex_id, e)
                        err_counts[ex_id] = err_counts.get(ex_id, 0) + 1
                    except Exception as e:
                        logger.warning(
                            "REST SNAPSHOT | %s %s failed: %s", ex_id, sym, e
                        )
                        err_counts[ex_id] = err_counts.get(ex_id, 0) + 1
            now = time.time()
            if self._should_log_rest_snapshot(
                now, last_log, settings.rest_snapshot_log_interval_seconds
            ):
                total_ok = sum(ok_counts.values())
                total_err = sum(err_counts.values())
                if settings.rest_snapshot_restart_enabled:
                    inputs = RestSnapshotInputs(
                        now_ts=now,
                        ok_count=total_ok,
                        err_count=total_err,
                        min_samples=settings.rest_snapshot_restart_min_samples,
                        err_rate_threshold=settings.rest_snapshot_restart_err_rate_threshold,
                        min_ok=settings.rest_snapshot_restart_min_ok,
                        last_restart_ts=self._rest_restart_last_ts,
                        cooldown_seconds=settings.rest_snapshot_restart_cooldown_seconds,
                    )
                    should_restart, reason = should_restart_rest_snapshot(inputs)
                    if should_restart:
                        self._rest_restart_last_ts = now
                        summary = self._format_rest_snapshot_summary(
                            ok_counts, err_counts
                        )
                        message = (
                            "REST RESTART | action=L0 reason=%s ok=%d err=%d summary=%s"
                            % (reason, total_ok, total_err, summary)
                        )
                        logger.warning(message)
                        if self.state and self.state.alert_service:
                            await self.state.alert_service.send_tech(message)
                        ok_counts = {}
                        err_counts = {}
                        last_log = now
                        continue
                summary = self._format_rest_snapshot_summary(ok_counts, err_counts)
                logger.info("REST SNAPSHOT | summary %s", summary)
                ok_counts = {}
                err_counts = {}
                last_log = now
            elapsed = time.time() - start
            sleep_for = max(0.0, settings.rest_snapshot_interval_seconds - elapsed)
            await asyncio.sleep(sleep_for)

    async def _run_native_ws_mode(
        self,
        symbols_per_exchange: Dict[str, List[str]],
    ) -> None:
        assert self.state is not None
        if self.gateway is None:
            raise RuntimeError("WSNATIVE | gateway not initialized")
        from parsertang.ws_native.depth_cache import DepthCache
        from parsertang.ws_native.router import build_snapshot
        from parsertang.ws_native.runner import NativeWsRunner

        loop = asyncio.get_running_loop()

        while True:
            native_exchanges = settings.ws_native_exchanges or []
            clients = self._build_native_ws_clients(native_exchanges)
            if not clients:
                raise RuntimeError("WSNATIVE | no supported exchanges configured")

            # In WS HYBRID mode, ccxt.pro exchanges can run alongside native WS.
            # Don't clobber allocations for non-native exchanges (e.g. gate).
            for ex_id in clients:
                self.state.ws_metrics.allocated_symbols[ex_id] = len(
                    symbols_per_exchange.get(ex_id, [])
                )

            depth_cache = DepthCache(
                self.gateway,
                refresh_seconds=settings.ws_native_depth_refresh_seconds,
                ttl_seconds=settings.ws_native_depth_ttl_seconds,
                limit=settings.orderbook_limit,
            )

            async def on_snapshot(ex_id: str, symbol: str, snap) -> None:
                self.state.ws_metrics.update_counters[ex_id] += 1
                self.state.ws_metrics.symbols_seen[ex_id].add(symbol)
                await self.state.update_orderbook(ex_id, symbol, snap)
                evaluate_arbitrage_for_symbol(symbol, self.state, loop)

            runner = NativeWsRunner(
                on_snapshot=on_snapshot,
                depth_cache=depth_cache,
                build_snapshot=build_snapshot,
                liquidity_window_pct=settings.liquidity_window_pct,
                trade_volume_usd=settings.trade_volume_usd,
            )

            tasks: list[asyncio.Task] = []
            symbol_counts = {
                ex_id: len(symbols_per_exchange.get(ex_id, []))
                for ex_id in clients.keys()
            }
            for ex_id, client in clients.items():
                symbols = symbols_per_exchange.get(ex_id, [])
                if not symbols:
                    continue
                tasks.append(
                    asyncio.create_task(
                        client.connect(symbols, runner.handle_event),
                        name=f"ws_native_{ex_id}",
                    )
                )

            if not tasks:
                logger.info("WSNATIVE | symbols_by_ex=%s", symbol_counts)
                logger.warning("WSNATIVE | no symbols allocated for native WS")
                return

            logger.info("WSNATIVE | symbols_by_ex=%s", symbol_counts)
            logger.info(
                "WSNATIVE | starting exchanges=%s",
                ",".join(sorted(clients.keys())),
            )

            async def _run_all() -> None:
                await asyncio.gather(*tasks)

            gather_task = asyncio.create_task(_run_all(), name="ws_native_gather")
            refresh_task = None
            if settings.symbol_refresh_enabled:
                refresh_task = asyncio.create_task(self._symbol_refresh_watcher())

            wait_tasks: list[asyncio.Task[Any]] = [gather_task]
            if refresh_task:
                wait_tasks.append(refresh_task)

            try:
                done, _ = await asyncio.wait(
                    wait_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                if refresh_task and refresh_task in done:
                    reason = refresh_task.result()
                    logger.info("SYMBOL REFRESH | resubscribing (reason=%s) [native]", reason)
                    gather_task.cancel()
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(gather_task, return_exceptions=True)
                    if refresh_task:
                        await asyncio.gather(refresh_task, return_exceptions=True)

                    symbols_per_exchange, markets = await self._refresh_symbols()
                    _ = markets
                    continue

                await gather_task
                return
            except asyncio.CancelledError:
                for task in tasks:
                    task.cancel()
                gather_task.cancel()
                if refresh_task:
                    refresh_task.cancel()
                await asyncio.gather(gather_task, return_exceptions=True)
                if refresh_task:
                    await asyncio.gather(refresh_task, return_exceptions=True)
                raise
            finally:
                if refresh_task:
                    refresh_task.cancel()
                    await asyncio.gather(refresh_task, return_exceptions=True)

    async def _run_websocket_mode(
        self,
        symbols_per_exchange: Dict[str, List[str]],
        rest_snapshot_symbols: Dict[str, List[str]] | None = None,
        *,
        ws_exchanges: list[str] | None = None,
        markets_ws: Dict[str, Dict] | None = None,
    ) -> None:
        """Run application in WebSocket mode (real-time orderbook updates).

        Args:
            symbols_per_exchange: Dict mapping exchange_id → list of symbols to subscribe
        """
        assert self.state is not None
        native_task: asyncio.Task | None = None
        while True:
            if self._should_use_native_ws(settings):
                native_exchanges = set(settings.ws_native_exchanges or [])
                try:
                    native_task = asyncio.create_task(
                        self._run_native_ws_mode(symbols_per_exchange),
                        name="ws_native_main",
                    )
                    native_task.add_done_callback(
                        lambda t: logger.exception(
                            "WSNATIVE | task crashed: %s",
                            t.exception(),
                        )
                        if t.exception()
                        else None
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("WSNATIVE | failed to start, falling back to ccxt.pro: %s", exc)
                else:
                    # Exclude native exchanges from ccxt.pro streams to avoid duplicate updates.
                    ws_exchanges = ws_exchanges or settings.exchanges
                    ws_exchanges = [ex for ex in ws_exchanges if ex not in native_exchanges]
                    logger.info(
                        "WS HYBRID | native=%s ccxt=%s",
                        ",".join(sorted(native_exchanges)) if native_exchanges else "-",
                        ",".join(ws_exchanges) if ws_exchanges else "-",
                    )
                    if not ws_exchanges:
                        if native_task:
                            await native_task
                        return
            # Initialize WebSocket streams
            proxy_config = self._build_proxy_config()
            try:
                preloaded = (
                    {ex_id: markets_ws[ex_id] for ex_id in ws_exchanges if markets_ws and ex_id in markets_ws}
                    if ws_exchanges
                    else {}
                )
                self.streams = Streams(
                    ws_exchanges,
                    proxy_config=proxy_config or None,
                    preloaded_markets_by_exchange=preloaded,
                )
            except RuntimeError:
                logger.error("WS initialization failed; falling back to REST mode")
                if self._refresh_trace:
                    self._report_refresh_trace()
                if self._ws_recover_future and not self._ws_recover_future.done():
                    self._ws_recover_future.set_result(False)
                    self._cancel_ws_recover_timeout()
                    if self._ws_recover_expected_exchanges is not None:
                        self._ws_recover_expected_exchanges = set()
                await self._run_rest_mode(symbols_per_exchange)
                return

            # Log WS initialization status
            self._log_ws_init_status()
            if self._refresh_trace:
                self._mark_refresh_trace("init")

            # Create orderbook callback with all dependencies
            loop = asyncio.get_running_loop()
            callback = self._create_orderbook_callback(loop)
            self._ws_on_update = callback
            self._ws_symbols_per_exchange = symbols_per_exchange
            rest_callback = self._create_rest_orderbook_callback(loop)

            # Subscribe to orderbooks (per-exchange workers)
            self._ws_clients = dict(self.streams.exchanges)
            self._ws_tasks = {
                ex_id: self.streams.start_exchange_worker(
                    ex_id,
                    ex,
                    symbols_per_exchange.get(ex_id, []),
                    callback,
                )
                for ex_id, ex in self.streams.exchanges.items()
            }
            orderbook_task = self._create_ws_gather_task(list(self._ws_tasks.values()))
            if self._refresh_trace:
                self._mark_refresh_trace("subscribe")
                self._refresh_trace_timeout_task = asyncio.create_task(
                    self._refresh_trace_timeout()
                )
            if self._ws_recover_pending:
                logger.info("L1 RECOVER | resubscribed to orderbooks")
                self._schedule_ws_recover_timeout()
                self._ws_recover_pending = False

            # Start liquidity summary task
            summary_task = self._create_background_task(
                log_liquidity_summary(self.state),
                name="log_liquidity_summary",
            )

            # Start cache cleaner task
            cache_cleaner_task = self._create_background_task(
                self._clear_exchange_caches(interval=10),
                name="clear_exchange_caches",
            )
            logger.info(
                "TASK CREATED | cache_cleaner_task exchanges=%d",
                len(self.streams.exchanges),
            )

            rest_snapshot_task = None
            if rest_snapshot_symbols:
                rest_snapshot_task = self._create_background_task(
                    self._rest_snapshot_loop(rest_snapshot_symbols, rest_callback),
                    name="rest_snapshot",
                )

            refresh_task = None
            if settings.symbol_refresh_enabled:
                refresh_task = asyncio.create_task(self._symbol_refresh_watcher())

            recover_task = asyncio.create_task(self._ws_recover_event.wait())

            # Optional: run timer for limited duration
            timer_task = await self._maybe_start_timer(
                [orderbook_task, summary_task, cache_cleaner_task]
            )

            wait_tasks: list[asyncio.Task[Any]] = [orderbook_task]
            if refresh_task:
                wait_tasks.append(refresh_task)
            if recover_task:
                wait_tasks.append(recover_task)
            if rest_snapshot_task:
                wait_tasks.append(rest_snapshot_task)
            if timer_task:
                wait_tasks.append(timer_task)

            try:
                done, _ = await asyncio.wait(
                    wait_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                if refresh_task and refresh_task in done:
                    reason = refresh_task.result()
                    self._start_refresh_trace(reason)
                    logger.info("SYMBOL REFRESH | resubscribing (reason=%s)", reason)
                    orderbook_task.cancel()
                    await self._cancel_ws_tasks()
                    summary_task.cancel()
                    cache_cleaner_task.cancel()
                    if rest_snapshot_task:
                        rest_snapshot_task.cancel()
                    await asyncio.gather(
                        orderbook_task,
                        summary_task,
                        cache_cleaner_task,
                        return_exceptions=True,
                    )
                    if rest_snapshot_task:
                        await asyncio.gather(rest_snapshot_task, return_exceptions=True)
                    if timer_task:
                        timer_task.cancel()
                        await asyncio.gather(timer_task, return_exceptions=True)
                    if self.streams:
                        await self.streams.close()
                        self._ws_clients.clear()
                        self._mark_refresh_trace("close")

                    symbols_per_exchange, markets = await self._refresh_symbols()
                    rest_snapshot_symbols = self._select_rest_snapshot_symbols(
                        markets, symbols_per_exchange
                    )
                    continue

                if recover_task in done:
                    logger.info("L1 RECOVER | resubscribing (guard)")
                    self._ws_recover_event.clear()
                    self._ws_recover_pending = True
                    orderbook_task.cancel()
                    await self._cancel_ws_tasks()
                    summary_task.cancel()
                    cache_cleaner_task.cancel()
                    if rest_snapshot_task:
                        rest_snapshot_task.cancel()
                    await asyncio.gather(
                        orderbook_task,
                        summary_task,
                        cache_cleaner_task,
                        return_exceptions=True,
                    )
                    if rest_snapshot_task:
                        await asyncio.gather(rest_snapshot_task, return_exceptions=True)
                    if refresh_task:
                        refresh_task.cancel()
                        await asyncio.gather(refresh_task, return_exceptions=True)
                    if timer_task:
                        timer_task.cancel()
                        await asyncio.gather(timer_task, return_exceptions=True)
                    if self.streams:
                        await self.streams.close()
                        self._ws_clients.clear()
                    continue

                await orderbook_task
            except asyncio.CancelledError:
                pass
            finally:
                for task in [summary_task, cache_cleaner_task]:
                    task.cancel()
                if refresh_task:
                    refresh_task.cancel()
                    await asyncio.gather(refresh_task, return_exceptions=True)
                if rest_snapshot_task:
                    rest_snapshot_task.cancel()
                    await asyncio.gather(rest_snapshot_task, return_exceptions=True)
                recover_task.cancel()
                await asyncio.gather(recover_task, return_exceptions=True)
                if timer_task:
                    timer_task.cancel()

                all_tasks = [summary_task, cache_cleaner_task] + self.background_tasks
                if timer_task:
                    all_tasks.append(timer_task)
                await asyncio.gather(*all_tasks, return_exceptions=True)
                await self._cancel_ws_tasks()
                self._ws_clients.clear()
            break

    async def _run_rest_mode(self, symbols_per_exchange: Dict[str, List[str]]) -> None:
        """Run application in REST polling mode (periodic orderbook fetch).

        Args:
            symbols_per_exchange: Dict mapping exchange_id → list of symbols to poll
        """
        assert self.state is not None
        assert self.gateway is not None

        logger.info("WS disabled; starting REST polling mode.")

        # Create orderbook callback
        loop = asyncio.get_running_loop()
        callback = self._create_orderbook_callback(loop)

        # REST polling task
        async def rest_polling_loop() -> None:
            while True:
                await asyncio.sleep(settings.check_interval_seconds)

                for ex_id, syms in symbols_per_exchange.items():
                    for s in syms:
                        try:
                            assert self.gateway is not None
                            bids, asks = self.gateway.fetch_order_book(
                                ex_id, s, limit=settings.orderbook_limit
                            )
                            if not bids or not asks:
                                continue

                            ob = {"bids": bids, "asks": asks}
                            callback(ex_id, s, ob)

                        except CircuitOpenError as e:
                            logger.debug(
                                "REST poll skipped %s: circuit OPEN (retry in %ds)",
                                e.exchange_id,
                                e.retry_after,
                            )
                            break  # Skip remaining symbols for this exchange
                        except Exception as e:
                            logger.debug("REST poll failed %s %s: %s", ex_id, s, e)

        rest_task = asyncio.create_task(rest_polling_loop())
        summary_task = self._create_background_task(
            log_liquidity_summary(self.state),
            name="log_liquidity_summary",
        )

        # Optional: run timer for limited duration
        timer_task = await self._maybe_start_timer([rest_task, summary_task])

        # Wait for completion
        try:
            await rest_task
        except asyncio.CancelledError:
            pass
        finally:
            summary_task.cancel()
            all_tasks = [summary_task] + self.background_tasks
            if timer_task:
                timer_task.cancel()
                all_tasks.append(timer_task)
            await asyncio.gather(*all_tasks, return_exceptions=True)

    async def _maybe_start_timer(
        self, tasks_to_cancel: List[asyncio.Task]
    ) -> asyncio.Task | None:
        """Start optional timer task for limited run duration.

        Args:
            tasks_to_cancel: Tasks to cancel when timer expires

        Returns:
            Timer task if RUN_DURATION_SECONDS configured, else None
        """
        if not settings.run_duration_seconds:
            return None

        logger.info("RUN TIMER: %ss", settings.run_duration_seconds)

        async def timer() -> None:
            try:
                await asyncio.sleep(float(settings.run_duration_seconds or 0))
            except asyncio.CancelledError:
                return
            logger.info("Run duration reached; shutting down.")
            for task in tasks_to_cancel:
                task.cancel()

        return asyncio.create_task(timer())

    def _log_ws_init_status(self) -> None:
        """Log WebSocket initialization status for all exchanges."""
        assert self.streams is not None

        init_ok = []
        init_failed = []
        init_unsupported = []

        for ex_id in sorted(self.streams.init_status):
            status = (self.streams.init_status.get(ex_id) or {}).get(
                "status", "unknown"
            )
            if status == "ok":
                init_ok.append(ex_id)
            elif status == "failed":
                init_failed.append(ex_id)
            elif status == "unsupported":
                init_unsupported.append(ex_id)
            else:
                init_failed.append(ex_id)

        logger.info(
            "WS INIT | ok=%s failed=%s unsupported=%s",
            ",".join(init_ok) if init_ok else "-",
            ",".join(init_failed) if init_failed else "-",
            ",".join(init_unsupported) if init_unsupported else "-",
        )

        # Log errors for failed exchanges
        for ex_id in init_failed:
            err = (self.streams.init_status.get(ex_id) or {}).get("error")
            if err:
                logger.error("WS INIT FAILED | %s: %s", ex_id, err)

    async def _clear_exchange_caches(self, interval: int = 5) -> None:
        """Clear orderbook caches for all ccxt.pro exchanges periodically.

        Prevents memory growth from ccxt.pro internal orderbook buffers.

        Args:
            interval: Seconds between cache clearing cycles
        """
        import gc

        assert self.streams is not None

        await asyncio.sleep(0)  # Yield to ensure task gets scheduled

        try:
            logger.info(
                "CACHE CLEANER STARTED | interval=%ds exchanges=%d",
                interval,
                len(self.streams.exchanges),
            )
        except Exception as e:
            logger.error("CACHE CLEANER FAILED TO START | %s", e)
            return

        while True:
            await asyncio.sleep(interval)
            cleared = 0
            for ex_id, ex in list(self.streams.exchanges.items()):
                try:
                    # Gate exchange requires special handling
                    if ex_id == "gate":
                        continue
                    if hasattr(ex, "orderbooks"):
                        ex.orderbooks.clear()
                        cleared += 1
                except Exception as e:  # noqa: BLE001
                    logger.warning("CACHE CLEAR ERROR | %s: %s", ex_id, e)

            gc.collect()  # Force garbage collection

            if cleared:
                logger.info(
                    "CACHE CLEAR | cleared %d exchange orderbook caches", cleared
                )

    async def _cleanup(self) -> None:
        """Cleanup on shutdown: stop services, close connections."""
        logger.info("Shutting down gracefully...")

        assert self.state is not None

        # Stop Telegram bot
        if self.state.bot:
            try:
                await self.state.bot.stop()
                logger.info("SimpleBot stopped")
            except Exception as e:
                logger.warning("Error stopping SimpleBot: %s", e)

        # Close WebSocket streams
        if self.streams:
            try:
                await self.streams.close()
                logger.info("Streams stopped")
            except Exception as e:
                logger.warning("Error stopping Streams: %s", e)

        # Close REST API gateway
        if self.gateway:
            self.gateway.close_all()
            logger.info("Exchange gateway closed")
