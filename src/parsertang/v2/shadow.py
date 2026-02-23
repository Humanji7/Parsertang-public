"""Shadow pipeline to run V2 side-by-side without affecting production flow."""

from __future__ import annotations

import logging
import time
from typing import Dict

from parsertang.config import settings

from .health_gate import HealthGate
from .health_metrics import compute_health_snapshot
from .queue import BoundedEventQueue
from .processor import Processor
from .guard import Guard, GuardMetrics, GuardDecision
from .models import Event

logger = logging.getLogger(__name__)


class ShadowPipeline:
    """Minimal shadow runner for V2 ingestion/processor/guard.

    - Accepts incoming orderbook updates (ex_id, symbol, ob dict with bids/asks/ts)
    - Transforms to Event and pushes into bounded queue
    - Processes queue, updates state, computes metrics, evaluates guard
    - Emits guard decisions to log (shadow namespace), no side-effects
    """

    def __init__(
        self,
        per_exchange_capacity: int = 200,
        guard: Guard | None = None,
        stale_timeout_seconds: float = 1.0,
        log_level: int = logging.DEBUG,
    ) -> None:
        self.queue = BoundedEventQueue(per_exchange_capacity=per_exchange_capacity)
        self.processor = Processor()
        self.guard = guard or Guard()
        self.stale_timeout_seconds = stale_timeout_seconds
        self.log_level = log_level
        self.health_gate = HealthGate(settings.v2_health_fresh_ratio_min)

    def on_orderbook(self, ex_id: str, symbol: str, ob: Dict) -> None:
        ts = ob.get("timestamp") or ob.get("datetime") or time.time() * 1000
        ev = Event(
            ex=ex_id,
            channel="orderbook",
            symbol=symbol,
            ts_ex=ts,
            ts_recv=time.time() * 1000,
            data=ob,
        )
        self.queue.push(ev)
        for ev in self.queue.drain():
            self.processor.handle(ev)
        self._maybe_log_decision()

    def _maybe_log_decision(self) -> None:
        now = time.time() * 1000
        metrics = self._metrics(now)
        decision: GuardDecision = self.guard.evaluate(metrics)
        snap = compute_health_snapshot(
            self.processor.snapshot_fresh(
                now_ts=now, max_age_ms=settings.v2_health_stale_seconds * 1000
            ),
            now_ms=now,
            max_age_ms=settings.v2_health_stale_seconds * 1000,
            expected_exchanges=["bybit", "okx", "mexc"],
        )
        health_decision = self.health_gate.evaluate(snap)
        logger.info(
            "V2 HEALTH | healthy=%s ratios=%s min=%.2f",
            health_decision.healthy,
            snap.fresh_ratio_by_ex,
            snap.fresh_ratio_min,
        )
        logger.log(
            self.log_level,
            "V2 SHADOW | level=%s reason=%s stale=%d multi_ex_symbols=%d tick_lag=%.2f queue=%d drops=%d actions=%s",
            decision.level,
            decision.reason,
            metrics.stale_exchanges,
            metrics.multi_ex_symbols,
            metrics.tick_lag,
            metrics.queue_depth,
            self.queue.stats().get("drops", 0),
            decision.actions,
        )

    def _metrics(self, now: float) -> GuardMetrics:
        state = self.processor.snapshot_fresh(
            now_ts=now, max_age_ms=self.stale_timeout_seconds * 1000
        )
        seen_ex = {ex for (ex, _sym) in state.keys()}
        # simple heuristic: consider 5 exchanges of interest
        exchanges = ["bybit", "okx", "kucoin", "gate", "mexc"]
        stale = sum(1 for ex in exchanges if ex not in seen_ex)
        lags = self.processor.lag(now_ts=now)
        max_lag = max(lags.values()) if lags else 0.0
        return GuardMetrics(
            stale_exchanges=stale,
            multi_ex_symbols=len({sym for (_ex, sym) in state.keys()}),
            tick_lag=max_lag,
            queue_depth=len(self.queue),
        )

    def stats(self) -> Dict[str, float | int | str]:
        """Expose current lightweight stats for debugging/inspection."""
        now = time.time() * 1000
        metrics = self._metrics(now)
        drops = self.queue.stats().get("drops", 0)
        decision = self.guard.evaluate(metrics)
        return {
            "level": decision.level.value,
            "stale_exchanges": metrics.stale_exchanges,
            "multi_ex_symbols": metrics.multi_ex_symbols,
            "tick_lag": metrics.tick_lag,
            "queue_depth": metrics.queue_depth,
            "drops": drops,
        }

    @staticmethod
    def level_from_str(level: str) -> int:
        upper = level.upper()
        if upper == "DEBUG":
            return logging.DEBUG
        if upper == "INFO":
            return logging.INFO
        if upper == "WARN" or upper == "WARNING":
            return logging.WARNING
        if upper == "ERROR":
            return logging.ERROR
        # OFF or unknown -> high level to suppress
        return logging.CRITICAL
