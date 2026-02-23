from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from parsertang.config import settings
from parsertang.fee_truth import fee_within_tolerance
from parsertang.withdrawal_fees import fetch_withdraw_fee_live, normalize_network_code
from parsertang.v2.validator import ValidationResult, Validator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AlertTruthEvidence:
    ts_wall: float
    symbol: str
    buy_ex: str
    sell_ex: str
    ws_buy: float
    ws_sell: float
    ws_ts_buy: float
    ws_ts_sell: float
    ws_age_buy_ms: int
    ws_age_sell_ms: int
    ws_skew_ms: int
    net_profit_pct: float
    network: str | None
    trade_fees_pct: float
    withdraw_fee_pct: float
    withdraw_fee_base: float
    fee_cache_age_seconds: float | None
    fee_live_base: float | None = None
    fee_live_source: str | None = None
    rest_buy: float | None = None
    rest_sell: float | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "ts_wall": self.ts_wall,
            "symbol": self.symbol,
            "buy_ex": self.buy_ex,
            "sell_ex": self.sell_ex,
            "ws_buy": self.ws_buy,
            "ws_sell": self.ws_sell,
            "ws_ts_buy": self.ws_ts_buy,
            "ws_ts_sell": self.ws_ts_sell,
            "ws_age_buy_ms": self.ws_age_buy_ms,
            "ws_age_sell_ms": self.ws_age_sell_ms,
            "ws_skew_ms": self.ws_skew_ms,
            "net_profit_pct": self.net_profit_pct,
            "network": self.network,
            "trade_fees_pct": self.trade_fees_pct,
            "withdraw_fee_pct": self.withdraw_fee_pct,
            "withdraw_fee_base": self.withdraw_fee_base,
            "fee_cache_age_seconds": self.fee_cache_age_seconds,
            "fee_live_base": self.fee_live_base,
            "fee_live_source": self.fee_live_source,
            "rest_buy": self.rest_buy,
            "rest_sell": self.rest_sell,
        }


async def verify_alert_truth_after_delay(
    *,
    validator: Validator,
    evidence: AlertTruthEvidence,
    delay_seconds: float | None = None,
) -> ValidationResult:
    """Re-validate an alert shortly after it was produced.

    This does NOT block alert sending. It is used to produce ALERTTRUTH OK/FAIL metrics
    that estimate how often alert numbers were anchored in reality.
    """
    delay = settings.alert_verify_delay_seconds if delay_seconds is None else float(delay_seconds)
    if delay > 0:
        await asyncio.sleep(delay)

    # Validator.validate is sync (ccxt); run in a thread to avoid blocking the event loop.
    result: ValidationResult = await asyncio.to_thread(
        validator.validate,
        symbol=evidence.symbol,
        buy_ex=evidence.buy_ex,
        sell_ex=evidence.sell_ex,
        buy_price=evidence.ws_buy,
        sell_price=evidence.ws_sell,
    )

    if result.ok and settings.alert_verify_fee_enabled:
        base = evidence.symbol.split("/")[0] if "/" in evidence.symbol else ""
        net = normalize_network_code(evidence.network or "", base)
        exchange = None
        if base and net and validator.gateway:
            exchanges = getattr(validator.gateway, "exchanges", None)
            if isinstance(exchanges, dict):
                exchange = exchanges.get(evidence.buy_ex)

        if exchange is not None and base and net:
            fee_live, fee_src = await asyncio.to_thread(
                fetch_withdraw_fee_live,
                exchange,
                currency=base,
                network=net,
            )
            if fee_live is None:
                logger.info(
                    "ALERTTRUTH FAIL | %s buy=%s sell=%s reason=fee_live_unavailable network=%s ws_fee_base=%.12f",
                    evidence.symbol,
                    evidence.buy_ex,
                    evidence.sell_ex,
                    net,
                    evidence.withdraw_fee_base,
                )
                return ValidationResult(
                    ok=False,
                    reason="fee_live_unavailable",
                    rest_buy=result.rest_buy,
                    rest_sell=result.rest_sell,
                )

            if not fee_within_tolerance(
                expected_fee_base=float(evidence.withdraw_fee_base),
                actual_fee_base=float(fee_live),
                tolerance_pct=float(settings.alert_verify_fee_tolerance_pct),
                tolerance_base=float(settings.alert_verify_fee_tolerance_base),
            ):
                logger.info(
                    "ALERTTRUTH FAIL | %s buy=%s sell=%s reason=fee_mismatch network=%s ws_fee_base=%.12f live_fee_base=%.12f source=%s",
                    evidence.symbol,
                    evidence.buy_ex,
                    evidence.sell_ex,
                    net,
                    float(evidence.withdraw_fee_base),
                    float(fee_live),
                    fee_src,
                )
                return ValidationResult(
                    ok=False,
                    reason="fee_mismatch",
                    rest_buy=result.rest_buy,
                    rest_sell=result.rest_sell,
                )

    if result.ok:
        logger.info(
            "ALERTTRUTH OK | %s buy=%s sell=%s reason=rest_consistent ws_buy=%.6f ws_sell=%.6f rest_buy=%.6f rest_sell=%.6f age_ms=%d/%d skew_ms=%d",
            evidence.symbol,
            evidence.buy_ex,
            evidence.sell_ex,
            evidence.ws_buy,
            evidence.ws_sell,
            result.rest_buy or 0.0,
            result.rest_sell or 0.0,
            evidence.ws_age_buy_ms,
            evidence.ws_age_sell_ms,
            evidence.ws_skew_ms,
        )
    else:
        rest_buy = f"{result.rest_buy:.6f}" if result.rest_buy else "-"
        rest_sell = f"{result.rest_sell:.6f}" if result.rest_sell else "-"
        logger.info(
            "ALERTTRUTH FAIL | %s buy=%s sell=%s reason=%s ws_buy=%.6f ws_sell=%.6f rest_buy=%s rest_sell=%s age_ms=%d/%d skew_ms=%d",
            evidence.symbol,
            evidence.buy_ex,
            evidence.sell_ex,
            result.reason,
            evidence.ws_buy,
            evidence.ws_sell,
            rest_buy,
            rest_sell,
            evidence.ws_age_buy_ms,
            evidence.ws_age_sell_ms,
            evidence.ws_skew_ms,
        )
    return result


def fee_cache_age_seconds(now_wall: float, *, fee_manager) -> float | None:
    if not fee_manager or not getattr(fee_manager, "cache", None):
        return None
    last_updated = getattr(fee_manager.cache, "last_updated", None)
    if not isinstance(last_updated, (int, float)):
        return None
    age = now_wall - float(last_updated)
    if not (age >= 0 and age < 365 * 24 * 3600):
        return None
    return float(age)
