import logging
import time


logger = logging.getLogger(__name__)


class NativeWsRunner:
    def __init__(
        self,
        *,
        on_snapshot,
        depth_cache=None,
        build_snapshot=None,
        liquidity_window_pct: float = 0.1,
        trade_volume_usd: float = 100.0,
    ):
        self.on_snapshot = on_snapshot
        self.depth_cache = depth_cache
        self.build_snapshot = build_snapshot
        self.liquidity_window_pct = liquidity_window_pct
        self.trade_volume_usd = trade_volume_usd
        self._depth_log_last_ts: dict[tuple[str, str], float] = {}
        self._depth_log_interval_s = 30.0

    def _emit_snapshot(self, snap):
        return self.on_snapshot(snap)

    def _should_log_depth(self, ex_id: str, reason: str, now: float) -> bool:
        key = (ex_id, reason)
        last = self._depth_log_last_ts.get(key, 0.0)
        if now - last < self._depth_log_interval_s:
            return False
        self._depth_log_last_ts[key] = now
        return True

    async def handle_event(self, ev):
        if not self.depth_cache or not self.build_snapshot:
            return None
        now = time.time()
        try:
            self.depth_cache.refresh(ev.ex, ev.symbol, now=now)
            depth = self.depth_cache.get(ev.ex, ev.symbol, now=now)
        except Exception as exc:
            if self._should_log_depth(ev.ex, type(exc).__name__, now):
                logger.warning(
                    "WSNATIVE DEPTH ERROR | ex=%s sym=%s err=%r", ev.ex, ev.symbol, exc
                )
            return None

        if not depth:
            if self._should_log_depth(ev.ex, "no_depth", now):
                logger.warning("WSNATIVE DEPTH EMPTY | ex=%s sym=%s", ev.ex, ev.symbol)
            return None
        snap = self.build_snapshot(
            ev,
            depth,
            liquidity_window_pct=self.liquidity_window_pct,
            trade_volume_usd=self.trade_volume_usd,
        )
        await self.on_snapshot(ev.ex, ev.symbol, snap)
        return snap
