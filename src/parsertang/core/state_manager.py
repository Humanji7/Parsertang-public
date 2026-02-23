"""Centralized state management for Parsertang.

Replaces global variables with thread-safe AppState context object.
All state mutations go through AppState methods with proper locking to prevent race conditions.

CRITICAL DESIGN NOTES:
- All shared state MUST be accessed through locks (no direct dict access)
- Orderbook updates are high-frequency — lock held for minimal time
- Metrics are aggregated periodically — lock only during read+reset
- Currency cache is read-heavy — consider RWLock if contention occurs
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsertang.alerts import AlertService, SimpleBot
    from parsertang.exchanges import ExchangeGateway
    from parsertang.fee_statistics import FeeStatistics
    from parsertang.trader import SimpleTrader
    from parsertang.withdrawal_fees import WithdrawalFeeManager


@dataclass
class OrderbookSnapshot:
    """Single orderbook snapshot with liquidity and slippage data.

    Attributes:
        best_bid: Best bid price (sell side)
        best_ask: Best ask price (buy side)
        bid_liq_usd: Bid-side liquidity in USD within configured window
        ask_liq_usd: Ask-side liquidity in USD within configured window
        bid_slip_pct: Estimated slippage % for selling at configured volume
        ask_slip_pct: Estimated slippage % for buying at configured volume
        ts: Timestamp (time.time()) when snapshot was created
    """

    best_bid: float
    best_ask: float
    bid_liq_usd: float
    ask_liq_usd: float
    bid_slip_pct: float
    ask_slip_pct: float
    ts: float


@dataclass
class WSMetrics:
    """WebSocket health metrics for monitoring connectivity.

    Attributes:
        update_counters: Count of orderbook updates per exchange (reset every 60s)
        symbols_seen: Unique symbols seen per exchange (reset every 60s)
        allocated_symbols: Expected symbol count per exchange (static, set at startup)
        stale_intervals: Consecutive 60s intervals with 0 updates per exchange
        last_logged: Timestamp when WS health was last logged
    """

    update_counters: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    symbols_seen: defaultdict[str, set] = field(
        default_factory=lambda: defaultdict(set)
    )
    allocated_symbols: dict[str, int] = field(default_factory=dict)
    stale_intervals: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    last_logged: float = 0.0


class AppState:
    """Centralized application state with thread-safe access.

    Single source of truth for all shared state in the application.
    Replaces 15+ global variables with explicit dependency injection.

    Usage:
        state = AppState()

        # Orderbook update (from WS callback)
        snapshot = OrderbookSnapshot(...)
        await state.update_orderbook("bybit", "BTC/USDT", snapshot)

        # Arbitrage evaluation (reads orderbooks)
        eligible = await state.get_eligible_orderbooks("BTC/USDT", 10000.0)

        # Metrics increment (from anywhere)
        async with state.metrics_lock:
            state.funnel_counters["liq_ok"] += 1

    Thread Safety:
        - All dict mutations MUST be protected by corresponding lock
        - Read-only access to immutable data (OrderbookSnapshot) is safe
        - Locks are async (asyncio.Lock) — use 'async with'
    """

    def __init__(self) -> None:
        # Orderbook state (high-frequency updates from WS)
        self.orderbooks: dict[tuple[str, str], OrderbookSnapshot] = {}
        self.orderbooks_lock = asyncio.Lock()
        self.state_last_cleanup = 0.0  # Last time stale orderbooks were removed

        # REST gateway (used by V2 validation)
        self.gateway: ExchangeGateway | None = None

        # Fee data (low-frequency updates from metadata refresh)
        self.currency_cache: dict[str, dict] = {}  # ex_id → {currency: metadata}
        self.currency_cache_lock = asyncio.Lock()
        self.fee_cache: dict[str, dict] = {}  # ex_id → {taker: %, maker: %}
        self.fee_cache_lock = asyncio.Lock()
        self.fee_manager: WithdrawalFeeManager | None = None

        # FUNNEL metrics (aggregated, reset every 60s)
        self.funnel_counters: defaultdict[str, int] = defaultdict(int)
        self.funnel_last_logged = 0.0

        # SPREAD distribution metrics (aggregated, reset every 300s)
        self.spread_buckets: defaultdict[str, int] = defaultdict(int)
        self.spread_last_logged = 0.0

        # ARB skip diagnostics (aggregated, reset every 60s)
        self.arb_skip_reasons: defaultdict[str, int] = defaultdict(int)
        self.arb_skip_samples: dict[str, tuple[str, str, str]] = {}

        # Fee validation diagnostics (aggregated, reset every 60s)
        self.fee_validation_reasons: defaultdict[str, int] = defaultdict(int)
        self.fee_validation_samples: dict[str, tuple[str, str, str]] = {}

        # Liquidity summary stats (aggregated, reset every 10s)
        self.stats: defaultdict[str, int] = defaultdict(int)

        # WS health metrics (aggregated, reset every 60s)
        self.ws_metrics = WSMetrics()

        # Metrics lock (protects all counters/buckets above)
        self.metrics_lock = asyncio.Lock()

        # Services (initialized in orchestrator, nullable until then)
        self.alert_service: AlertService | None = None
        self.trader: SimpleTrader | None = None
        self.bot: SimpleBot | None = None
        self.fee_stats: FeeStatistics | None = None

        # Alert deduplication (read+write from opportunity evaluator)
        # Key: (symbol, buy_ex, sell_ex), Value: (monotonic_ts, net_profit_pct)
        self.last_alert_ts: dict[tuple[str, str, str], tuple[float, float]] = {}
        self.alert_lock = asyncio.Lock()

        # ARB OK log deduplication (reduces log spam from 90/sec to 1/10sec)
        # Key: (symbol, buy_ex, sell_ex), Value: monotonic_ts
        self.last_arb_ok_log: dict[tuple[str, str, str], float] = {}

        # V2 validation stale symbol tracking (fail-closed safety)
        self.validation_stale_counts: defaultdict[str, int] = defaultdict(int)
        # V2 validation REST divergence tracking (symbol quarantine)
        self.validation_rest_fail_counts: defaultdict[str, int] = defaultdict(int)
        self.validation_symbol_blacklist: dict[str, float] = {}

    async def update_orderbook(
        self,
        ex_id: str,
        symbol: str,
        snapshot: OrderbookSnapshot,
    ) -> None:
        """Thread-safe orderbook update.

        Called from WS callback (on_orderbook_update) at high frequency.
        Lock is held for minimal time — just dict assignment.

        Args:
            ex_id: Exchange ID (e.g., "bybit")
            symbol: Trading pair (e.g., "BTC/USDT")
            snapshot: Precomputed orderbook snapshot with liquidity/slippage
        """
        async with self.orderbooks_lock:
            self.orderbooks[(ex_id, symbol)] = snapshot

    async def get_eligible_orderbooks(
        self,
        symbol: str,
        liquidity_threshold: float,
    ) -> list[tuple[str, OrderbookSnapshot]]:
        """Get eligible orderbooks for symbol with liquidity filter.

        Called from evaluate_arbitrage_for_symbol to find buy/sell candidates.
        Lock is held while filtering — keep logic simple.

        Args:
            symbol: Trading pair to filter for
            liquidity_threshold: Minimum USD liquidity (both bid and ask)

        Returns:
            List of (exchange_id, snapshot) tuples that pass liquidity filter
        """
        async with self.orderbooks_lock:
            eligible = []
            for (ex_id, sym), snapshot in self.orderbooks.items():
                if sym != symbol:
                    continue
                if snapshot.bid_liq_usd < liquidity_threshold:
                    continue
                if snapshot.ask_liq_usd < liquidity_threshold:
                    continue
                eligible.append((ex_id, snapshot))
            return eligible

    async def cleanup_stale_orderbooks(self, ttl_seconds: float) -> int:
        """Remove stale orderbook entries older than TTL.

        Called periodically (every 30s) from orderbook_processor to prevent memory leak.
        Current implementation: TTL = 120s, cleanup interval = 30s.

        Args:
            ttl_seconds: Time-to-live in seconds (stale threshold)

        Returns:
            Number of stale entries removed
        """
        now = time.time()
        async with self.orderbooks_lock:
            stale_keys = [
                k for k, v in self.orderbooks.items() if now - v.ts > ttl_seconds
            ]
            for k in stale_keys:
                del self.orderbooks[k]
            return len(stale_keys)

    async def update_currency_cache(
        self,
        ex_id: str,
        currencies: dict,
    ) -> None:
        """Thread-safe currency metadata update.

        Called from metadata_refresher (every 60s) after fetching from exchange API.

        Args:
            ex_id: Exchange ID
            currencies: Currency metadata dict from ccxt.fetch_currencies()
        """
        async with self.currency_cache_lock:
            self.currency_cache[ex_id] = currencies

    async def get_currency_cache(self, ex_id: str) -> dict:
        """Thread-safe read from currency cache.

        Args:
            ex_id: Exchange ID

        Returns:
            Currency metadata dict (empty if not cached)
        """
        async with self.currency_cache_lock:
            return self.currency_cache.get(ex_id, {})

    async def update_fee_cache(
        self,
        ex_id: str,
        taker_pct: float,
        maker_pct: float,
    ) -> None:
        """Thread-safe fee cache update.

        Args:
            ex_id: Exchange ID
            taker_pct: Taker fee percentage (e.g., 0.075 for 0.075%)
            maker_pct: Maker fee percentage
        """
        async with self.fee_cache_lock:
            self.fee_cache[ex_id] = {"taker": taker_pct, "maker": maker_pct}

    async def get_fee_cache(self, ex_id: str) -> dict:
        """Thread-safe read from fee cache.

        Args:
            ex_id: Exchange ID

        Returns:
            Fee dict with "taker" and "maker" keys (empty if not cached)
        """
        async with self.fee_cache_lock:
            return self.fee_cache.get(ex_id, {})

    async def cleanup_stale_alerts(self, ttl_seconds: float = 14400) -> int:
        """Remove stale alert deduplication entries.

        Called periodically (every 300s) to prevent unbounded memory growth.
        Default TTL = 4 hours (14400s).

        Args:
            ttl_seconds: Alert entry time-to-live

        Returns:
            Number of stale entries removed
        """
        now = time.monotonic()
        async with self.alert_lock:
            stale = [
                k for k, (ts, _) in self.last_alert_ts.items() if now - ts > ttl_seconds
            ]
            for k in stale:
                del self.last_alert_ts[k]
            return len(stale)

    async def check_and_update_alert(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        net_profit_pct: float,
        cooldown_seconds: float,
        dedup_threshold_pct: float,
    ) -> bool:
        """Check if alert should be sent and update deduplication state.

        Thread-safe check-and-set for alert deduplication logic.
        Returns True if alert should be sent (passed cooldown or profit change threshold).

        Args:
            symbol: Trading pair
            buy_exchange: Buy exchange ID
            sell_exchange: Sell exchange ID
            net_profit_pct: Current net profit percentage
            cooldown_seconds: Minimum time between alerts for same opportunity
            dedup_threshold_pct: Minimum profit change to bypass cooldown

        Returns:
            True if alert should be sent, False if deduplicated
        """
        now = time.monotonic()
        alert_key = (symbol, buy_exchange, sell_exchange)

        async with self.alert_lock:
            last_alert = self.last_alert_ts.get(alert_key)

            if last_alert is not None:
                last_ts, last_net_profit = last_alert
                time_ok = (now - last_ts) >= cooldown_seconds
                profit_change_pct = abs(net_profit_pct - last_net_profit)
                profit_ok = profit_change_pct > dedup_threshold_pct

                if not time_ok and not profit_ok:
                    return False  # Deduplicated

            # Update state (alert will be sent)
            self.last_alert_ts[alert_key] = (now, net_profit_pct)
            return True
