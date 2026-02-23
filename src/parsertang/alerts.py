import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Optional, Literal

import httpx
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler

try:
    from telegram.request import HTTPXRequest
except Exception:  # pragma: no cover
    HTTPXRequest = None  # type: ignore[assignment]

from .config import settings


logger = logging.getLogger(__name__)


def make_alert_evidence_id(
    *, now_ts: float, symbol: str, buy_exchange: str, sell_exchange: str
) -> str:
    """Create a human-friendly id for manual verification & log correlation."""
    ts = datetime.fromtimestamp(float(now_ts), tz=timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    sym = symbol.replace("/", "").replace(":", "_")
    return f"{ts}-{sym}-{buy_exchange}-{sell_exchange}"


def pick_alert_channel(net_profit_pct: float) -> Literal["trade", "tech"]:
    """Choose where to route an alert based on net profit.

    - Base gating is still done by `MIN_NET_PROFIT` (settings.min_net_profit).
    - If `MIN_NET_PROFIT_TRADE` is set, only higher-profit alerts go to the
      trader channel; the rest are routed to the tech channel.
    """
    trade_threshold = (
        settings.min_net_profit_trade
        if settings.min_net_profit_trade is not None
        else settings.min_net_profit
    )
    return "trade" if float(net_profit_pct) >= float(trade_threshold) else "tech"


class AlertService:
    def __init__(
        self,
        token: Optional[str] = None,
        chat_id: Optional[str] = None,
        tech_chat_id: Optional[str] = None,
    ):
        self.token = (
            token or settings.telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        )
        self.chat_id = (
            chat_id or settings.telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID")
        )
        self.tech_chat_id = (
            tech_chat_id
            or settings.telegram_tech_chat_id
            or os.getenv("TELEGRAM_TECH_CHAT_ID")
        )
        self.bot: Optional[Bot] = None
        # Lazily created in the running event loop to avoid "no current event loop"
        # errors in sync contexts (e.g. truth_aggregator startup).
        self._send_lock: asyncio.Lock | None = None
        if self.token and self.chat_id:
            try:
                # DNS on some VPS providers can be intermittently flaky (seen as
                # `curl: (28) Resolving timed out ...`). If the operator sets
                # RES_OPTIONS, glibc resolver will fail over between nameservers
                # faster. We don't set it automatically here to avoid changing
                # global resolver behavior silently.
                #
                # Recommended service-level setting:
                #   RES_OPTIONS=attempts:2 timeout:1

                # python-telegram-bot defaults are extremely strict:
                # - connection_pool_size=1, pool_timeout=1s, read/connect/write=5s
                # Under bursty alert/tech traffic this causes pool exhaustion and
                # frequent timeouts. Use a slightly larger pool + more forgiving
                # timeouts, and serialize sends via a lock (see below).
                if HTTPXRequest is not None:
                    request = HTTPXRequest(
                        connection_pool_size=8,
                        pool_timeout=20.0,
                        connect_timeout=20.0,
                        read_timeout=30.0,
                        write_timeout=30.0,
                        httpx_kwargs={
                            "transport": httpx.AsyncHTTPTransport(retries=3),
                        },
                    )
                    self.bot = Bot(self.token, request=request)
                else:  # pragma: no cover
                    self.bot = Bot(self.token)
            except Exception as e:
                logger.warning("Failed to init Telegram bot: %s", e)

    def send(self, text: str):
        """Send alert via Telegram (synchronous wrapper for async send_message)."""
        if not self.bot or not self.chat_id:
            logger.info("ALERT: %s", text)
            return
        try:
            # Get or create event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop - create new one and run
                asyncio.run(self._send_async(text))
            else:
                # Running loop exists - schedule as task
                task = loop.create_task(self._send_async(text))
                task.add_done_callback(self._handle_task_exception)
        except Exception as e:
            logger.error("Failed to send Telegram alert: %s", e)

    @staticmethod
    def _handle_task_exception(task: asyncio.Task) -> None:
        """Callback to log exceptions from fire-and-forget tasks."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("Telegram alert task failed: %s", exc)

    async def _send_async(self, text: str):
        """Internal async method for sending Telegram message."""
        if self.bot is None or self.chat_id is None:
            logger.warning("Cannot send message: bot or chat_id is None")
            return
        # Serialize outbound Telegram API calls to avoid HTTP connection pool
        # exhaustion under bursty alert/tech traffic.
        lock = self._send_lock
        if lock is None:
            lock = asyncio.Lock()
            self._send_lock = lock
        async with lock:
            last_error: Exception | None = None
            max_attempts = 6
            # Hard cap per attempt. In production we've seen rare cases where the
            # underlying DNS/connect path stalls far longer than httpx timeouts,
            # which would block the global send lock and backlog alerts.
            per_attempt_hard_timeout_s = 25.0
            for attempt in range(max_attempts):
                try:
                    msg = await asyncio.wait_for(
                        self.bot.send_message(
                            chat_id=self.chat_id, text=text, parse_mode=ParseMode.HTML
                        ),
                        timeout=per_attempt_hard_timeout_s,
                    )
                    logger.info(
                        "ALERT SENT | chat_id=%s message_id=%s",
                        self.chat_id,
                        getattr(msg, "message_id", None),
                    )
                    return
                except Exception as e:
                    last_error = e
                    if attempt + 1 < max_attempts:
                        # Exponential backoff with tiny jitter:
                        # 0s, ~1s, ~2s, ~4s, ~8s, (final attempt)
                        delay = 0.0 if attempt == 0 else min(2 ** (attempt - 1), 8.0)
                        delay += random.random() * 0.25
                        logger.warning(
                            "Telegram send_message attempt %s/%s failed: %s: %s (retry_in=%.2fs)",
                            attempt + 1,
                            max_attempts,
                            type(e).__name__,
                            e,
                            delay,
                        )
                        await asyncio.sleep(delay)
            logger.error(
                "Telegram send_message error after %s attempts: %s: %s",
                max_attempts,
                type(last_error).__name__ if last_error else None,
                last_error,
                exc_info=True,
            )

    async def send_tech(self, text: str) -> None:
        """Send message to technical channel (async).

        Used for daily fee reports and other technical notifications.

        Args:
            text: Message text to send
        """
        if self.bot is None:
            logger.warning("Cannot send tech message: bot is None")
            return

        target_chat_id = self.tech_chat_id
        if target_chat_id is None:
            logger.warning("Cannot send tech message: no tech_chat_id configured")
            return

        lock = self._send_lock
        if lock is None:
            lock = asyncio.Lock()
            self._send_lock = lock
        async with lock:
            last_error: Exception | None = None
            max_attempts = 6
            per_attempt_hard_timeout_s = 25.0
            for attempt in range(max_attempts):
                try:
                    msg = await asyncio.wait_for(
                        self.bot.send_message(
                            chat_id=target_chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                        ),
                        timeout=per_attempt_hard_timeout_s,
                    )
                    logger.info(
                        "TECH SENT | chat_id=%s message_id=%s",
                        target_chat_id,
                        getattr(msg, "message_id", None),
                    )
                    return
                except Exception as e:
                    last_error = e
                    if attempt + 1 < max_attempts:
                        delay = 0.0 if attempt == 0 else min(2 ** (attempt - 1), 8.0)
                        delay += random.random() * 0.25
                        logger.warning(
                            "Telegram send_tech attempt %s/%s failed: %s: %s (retry_in=%.2fs)",
                            attempt + 1,
                            max_attempts,
                            type(e).__name__,
                            e,
                            delay,
                        )
                        await asyncio.sleep(delay)
            logger.error(
                "Telegram send_tech error after %s attempts: %s: %s",
                max_attempts,
                type(last_error).__name__ if last_error else None,
                last_error,
                exc_info=True,
            )


def format_alert(
    symbol: str,
    buy_exchange: str,
    buy_price: float,
    sell_exchange: str,
    sell_price: float,
    gross_spread_pct: float,
    trade_fees_pct: float,
    withdraw_fee_pct: float,
    net_profit_pct: float,
    bid_liq_usd: float,
    ask_liq_usd: float,
    network: str,
    withdrawal_fee_base: float = 0.0,
    fee_confidence: str = "HIGH",
    fee_verified_at: Optional[str] = None,
    net_profit_transfer_pct: float | None = None,
    net_profit_funded_pct: float | None = None,
):
    evidence_id = make_alert_evidence_id(
        now_ts=time.time(),
        symbol=symbol,
        buy_exchange=buy_exchange,
        sell_exchange=sell_exchange,
    )
    # Extract base currency from symbol (e.g., "LTC/USDT" -> "LTC")
    base_currency = symbol.split("/")[0] if "/" in symbol else "BASE"

    # Convert base currency fee to USD equivalent
    withdrawal_fee_usd = withdrawal_fee_base * buy_price

    # Format withdrawal fee: always show base currency amount + USD equivalent
    withdrawal_display = f"{withdraw_fee_pct:.2f}%"
    if withdrawal_fee_usd < 0.01:  # Less than 1 cent USD
        withdrawal_display = f"{withdrawal_fee_base:.4f} {base_currency} (${withdrawal_fee_usd:.4f}, {withdraw_fee_pct:.3f}%)"
    elif withdraw_fee_pct < 0.01:  # Less than 0.01%
        withdrawal_display = f"{withdrawal_fee_base:.4f} {base_currency} (${withdrawal_fee_usd:.2f}, {withdraw_fee_pct:.3f}%)"
    else:
        withdrawal_display = f"{withdrawal_fee_base:.4f} {base_currency} (${withdrawal_fee_usd:.2f}, {withdraw_fee_pct:.2f}%)"

    # Build base message
    net_line = f"Net Profit: {net_profit_pct:.2f}%"
    if net_profit_transfer_pct is not None and net_profit_funded_pct is not None:
        net_line = (
            f"Net Profit (Funded): {net_profit_funded_pct:.2f}%\n"
            f"Net Profit (Transfer): {net_profit_transfer_pct:.2f}%"
        )

    message = (
        f"ID: {evidence_id}\n"
        f"Арбитраж: {symbol}\n"
        f"Buy: {buy_exchange} (${buy_price:,.4f}), Sell: {sell_exchange} (${sell_price:,.4f})\n"
        f"Gross Spread: {gross_spread_pct:.2f}%\n"
        f"Fees: {(trade_fees_pct + withdraw_fee_pct):.2f}% (Trade: {trade_fees_pct:.2f}%, Withdrawal: {withdrawal_display} via {network})\n"
        f"{net_line}\n"
        f"Liquidity: Bid {bid_liq_usd:,.0f} USD, Ask {ask_liq_usd:,.0f} USD"
        f"\nVerify in UI:\n"
        f"- BUY: {buy_exchange} ASK\n"
        f"- SELL: {sell_exchange} BID"
    )

    # Add confidence warning if LOW or MEDIUM
    if fee_confidence == "LOW":
        message += "\n\n⚠️ WARNING: Withdrawal fee unknown (confidence=LOW). Profit may be OPTIMISTIC!"
    elif fee_confidence == "MEDIUM":
        from parsertang.static_withdrawal_fees import (
            calculate_fee_age_days,
            get_fee_age_warning,
        )

        age_warning = ""
        if fee_verified_at:
            age_days = calculate_fee_age_days(fee_verified_at)
            age_warning = get_fee_age_warning(age_days)

        if age_warning:
            message += f"\n\n{age_warning}"
        else:
            message += "\n\nℹ️ Note: Using fallback withdrawal fee (confidence=MEDIUM)."

    return message


class SimpleBot:
    """Telegram polling bot for МДРК trading (Phase R0-R3).

    Supports commands:
    - /ping: Health check
    - /status: Show configuration
    - /cycles: List active trading cycles
    - /confirm <cycle_id>: Confirm LEG2 withdrawal (Phase R3)
    - /cancel <cycle_id>: Cancel an active cycle (Phase R3)
    """

    def __init__(self, trader=None, gateway=None):
        """Initialize bot.

        Args:
            trader: Optional SimpleTrader instance for LEG2 confirmations
            gateway: Optional ExchangeGateway for circuit breaker health status
        """
        if not settings.telegram_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required for bot operation")
        if not settings.telegram_chat_id:
            raise ValueError("TELEGRAM_CHAT_ID is required for bot operation")

        self.trader = trader
        self.gateway = gateway
        builder = Application.builder().token(settings.telegram_bot_token)
        if HTTPXRequest is not None:
            # Long-polling is sensitive to transient network hiccups. Use a slightly
            # larger pool + more forgiving timeouts to avoid frequent poll failures.
            builder = builder.request(
                HTTPXRequest(
                    connection_pool_size=4,
                    pool_timeout=20.0,
                    connect_timeout=20.0,
                    read_timeout=60.0,
                    write_timeout=30.0,
                    httpx_kwargs={
                        "transport": httpx.AsyncHTTPTransport(retries=3),
                    },
                )
            )
        self.app = builder.build()

        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("ping", self.cmd_ping))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("cycles", self.cmd_cycles))
        self.app.add_handler(CommandHandler("confirm", self.cmd_confirm))
        self.app.add_handler(CommandHandler("cancel", self.cmd_cancel))

        self._started = False

        logger.info(
            "SimpleBot initialized with authorized chat_id: %s",
            settings.telegram_chat_id,
        )

    async def start(self):
        """Start the bot in non-blocking async mode."""
        logger.info("Starting SimpleBot (async mode)...")
        await self.app.initialize()
        await self.app.start()
        # Updater may be None if not available; run_polling creates it automatically.
        # In async mode we rely on updater to poll updates without blocking.
        if self.app.updater:
            await self.app.updater.start_polling()
        self._started = True

    async def stop(self):
        """Stop the bot if it was started in async mode."""
        logger.info("Stopping SimpleBot (async mode)...")
        try:
            if self.app.updater:
                await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        finally:
            self._started = False

    def run(self):
        """Start the bot in polling mode (blocking)."""
        logger.info("Starting SimpleBot in polling mode...")
        try:
            self.app.run_polling()
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error("Bot error: %s", e)
            raise

    async def _authorized(self, update) -> bool:
        """Check if the update comes from an authorized chat_id.

        Authorization order:
        1. Check access_control_ids (multi-user support)
        2. Fallback to telegram_chat_id (single user, backward compatible)
        """
        user_id = str(update.effective_chat.id)

        # Check multi-user access list first
        access_ids = settings.get_access_control_ids()
        if access_ids:
            authorized = user_id in access_ids
            if authorized:
                logger.debug("User %s authorized via access_control_ids", user_id)
            return authorized

        # Fallback to single chat_id (backward compatible)
        return user_id == str(settings.telegram_chat_id)

    async def cmd_ping(self, update, context):
        """Health check command: /ping"""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return
        await update.message.reply_text("🟢 Bot is alive")
        logger.info("Bot ping from authorized user: %s", update.effective_chat.id)

    async def cmd_start(self, update, context):
        """/start: show minimal help and confirm bot is reachable."""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return
        await update.message.reply_text(
            "🟢 Parsertang bot is alive.\n\nCommands:\n"
            "/ping — health check\n"
            "/status — current config snapshot\n"
            "/cycles — list active cycles"
        )

    async def cmd_status(self, update, context):
        """Status command: /status - shows current configuration"""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return

        status = {
            "ws_enabled": settings.ws_enabled,
            "exchanges": settings.exchanges,
            "min_profit": settings.min_net_profit,
            "trading_enabled": settings.trading_enabled,
            "dry_run_mode": settings.dry_run_mode,
        }

        # Build circuit breaker health summary
        health_lines = []
        if self.gateway:
            health = self.gateway.get_health_summary()
            for ex_id, info in sorted(health.items()):
                state = info.get("state", "unknown")
                if state == "open":
                    retry = info.get("retry_after_seconds", 0)
                    health_lines.append(f"🔴 {ex_id}: OPEN (retry in {retry}s)")
                elif state == "half_open":
                    health_lines.append(f"🟡 {ex_id}: HALF_OPEN (probing...)")
                else:
                    failures = info.get("failure_count", 0)
                    if failures > 0:
                        health_lines.append(f"🟢 {ex_id}: OK ({failures} failures)")
                    else:
                        health_lines.append(f"🟢 {ex_id}: OK")

        # Build response message
        status_text = f"📄 Status:\n<pre>{json.dumps(status, indent=2)}</pre>"
        if health_lines:
            status_text += "\n\n🛡️ Circuit Breaker:\n" + "\n".join(health_lines)

        await update.message.reply_text(status_text, parse_mode="HTML")
        logger.info(
            "Bot status requested from authorized user: %s", update.effective_chat.id
        )

    async def cmd_cycles(self, update, context):
        """Cycles command: /cycles - list active trading cycles"""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return

        if not self.trader:
            await update.message.reply_text("⚠️ No trader instance connected")
            return

        active_cycles = self.trader.active_cycles

        if not active_cycles:
            await update.message.reply_text("📊 No active cycles")
            return

        # Build cycles summary
        lines = ["📊 Active Cycles:"]
        for cycle_id, cycle in active_cycles.items():
            opp = cycle.opportunity
            if opp:
                lines.append(
                    f"\n🔄 <code>{cycle_id}</code>\n"
                    f"  State: <b>{cycle.state.value}</b>\n"
                    f"  Symbol: {opp.symbol}\n"
                    f"  Route: {opp.buy_exchange} → {opp.sell_exchange}\n"
                    f"  Net Profit: {opp.net_profit_pct:.2f}%\n"
                    f"  Network: {opp.network or 'N/A'}\n"
                    f"  Duration: {cycle.duration_seconds():.1f}s"
                )
            else:
                lines.append(f"\n🔄 <code>{cycle_id}</code>: {cycle.state.value}")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        logger.info(
            "Bot cycles requested from authorized user: %s", update.effective_chat.id
        )

    async def cmd_confirm(self, update, context):
        """Confirm command: /confirm <cycle_id> - approve LEG2 withdrawal"""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return

        if not self.trader:
            await update.message.reply_text("⚠️ No trader instance connected")
            return

        # Parse cycle_id argument
        if not context.args:
            await update.message.reply_text(
                "❌ Usage: /confirm <cycle_id>\n\nUse /cycles to see active cycles"
            )
            return

        cycle_id = context.args[0]

        # Check if cycle exists
        if cycle_id not in self.trader.active_cycles:
            await update.message.reply_text(
                f"❌ Cycle <code>{cycle_id}</code> not found", parse_mode="HTML"
            )
            return

        cycle = self.trader.active_cycles[cycle_id]

        # Check if cycle is in LEG2_WAIT state
        from parsertang.trade_models import CycleState

        if cycle.state != CycleState.LEG2_WAIT:
            await update.message.reply_text(
                f"❌ Cycle <code>{cycle_id}</code> is in state <b>{cycle.state.value}</b>, not LEG2_WAIT",
                parse_mode="HTML",
            )
            return

        # Confirm the withdrawal
        success = await self.trader.confirm_leg2_withdrawal(cycle_id)

        if success:
            await update.message.reply_text(
                f"✅ LEG2 withdrawal confirmed for cycle <code>{cycle_id}</code>\n"
                f"🔄 Proceeding with withdrawal...",
                parse_mode="HTML",
            )
            logger.info(
                "LEG2 withdrawal confirmed for cycle %s by user %s",
                cycle_id,
                update.effective_chat.id,
            )
        else:
            await update.message.reply_text(
                f"❌ Failed to confirm LEG2 for cycle <code>{cycle_id}</code>",
                parse_mode="HTML",
            )

    async def cmd_cancel(self, update, context):
        """Cancel command: /cancel <cycle_id> - cancel an active cycle"""
        if not await self._authorized(update):
            await update.message.reply_text("unauthorized")
            return

        if not self.trader:
            await update.message.reply_text("⚠️ No trader instance connected")
            return

        # Parse cycle_id argument
        if not context.args:
            await update.message.reply_text(
                "❌ Usage: /cancel <cycle_id>\n\nUse /cycles to see active cycles"
            )
            return

        cycle_id = context.args[0]

        # Check if cycle exists
        if cycle_id not in self.trader.active_cycles:
            await update.message.reply_text(
                f"❌ Cycle <code>{cycle_id}</code> not found", parse_mode="HTML"
            )
            return

        # Cancel the cycle
        success = await self.trader.cancel_cycle(cycle_id)

        if success:
            await update.message.reply_text(
                f"✅ Cycle <code>{cycle_id}</code> cancelled", parse_mode="HTML"
            )
            logger.info(
                "Cycle %s cancelled by user %s", cycle_id, update.effective_chat.id
            )
        else:
            await update.message.reply_text(
                f"❌ Failed to cancel cycle <code>{cycle_id}</code>", parse_mode="HTML"
            )
