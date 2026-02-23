from __future__ import annotations

import logging
import sys
from typing import List

from parsertang.config import WS_ID_ALIASES, settings


def run() -> int:
    """Run diagnostics and return exit code (0 = success, 1 = error)."""
    # Setup clean output (no timestamps, just messages)
    logger = logging.getLogger("parsertang.diagnostics")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)

    logger.info("=== Parsertang Diagnostics ===")
    logger.info("")

    # Check for critical configuration errors first
    critical_errors = check_critical_errors()
    if critical_errors:
        for error in critical_errors:
            logger.info(f"❌ {error}")
        logger.info("")
        logger.info("❌ Critical errors detected")
        return 1

    # Try to import ccxt.pro
    ccxtpro = None
    _source = None

    try:
        import ccxt.pro as ccxtpro  # type: ignore[attr-defined]

        _source = "ccxt.pro"
    except Exception:  # noqa: BLE001
        try:
            import ccxtpro  # type: ignore

            _source = "ccxtpro"
            ccxtpro = ccxtpro  # Assign to local variable
        except Exception:  # noqa: BLE001
            ccxtpro = None

    # Check ccxt.pro availability
    if ccxtpro:
        version = getattr(ccxtpro, "__version__", "unknown")
        try:
            # Try to get source path
            import inspect

            source_file = inspect.getfile(ccxtpro)
            logger.info(f"✅ ccxt.pro: Available (v{version})")
            logger.info(f"   Source: {source_file}")
        except Exception:  # noqa: BLE001
            logger.info(f"✅ ccxt.pro: Available (v{version})")
        logger.info("")

        # Check WebSocket support for configured exchanges
        available_classes = [name for name in dir(ccxtpro) if name.islower()]
        ws_supported_count = 0
        ws_status_lines = []

        for ex_id in settings.exchanges:
            alias = WS_ID_ALIASES.get(ex_id, ex_id)
            if alias in available_classes:
                ws_status_lines.append(f"   - {ex_id}: watchOrderBook supported")
                ws_supported_count += 1
            else:
                ws_status_lines.append(f"   - {ex_id}: NOT supported")

        if ws_supported_count > 0:
            logger.info("✅ WebSocket Support:")
        else:
            logger.info("⚠️  WebSocket Support:")

        for line in ws_status_lines:
            logger.info(line)
        logger.info("")

    else:
        # ccxt.pro not available - fallback mode
        logger.info("⚠️  ccxt.pro: Not available (using REST fallback)")
        logger.info("   Reason: Import failed or unsupported")
        logger.info("")

    # Display configuration
    logger.info("⚙️  Configuration:")

    ws_status = "true" if settings.ws_enabled else "false"
    if not ccxtpro and settings.ws_enabled:
        ws_status += " (forced to false by ccxt.pro unavailability)"
    logger.info(f"   WS_ENABLED: {ws_status}")

    logger.info(
        f"   TRADING_ENABLED: {'true' if settings.trading_enabled else 'false'}"
    )
    logger.info(f"   MIN_NET_PROFIT: {settings.min_net_profit}%")
    logger.info(f"   EXCHANGES: {', '.join(settings.exchanges)}")
    logger.info(f"   MAX_SYMBOLS_PER_EXCHANGE: {settings.max_symbols_per_exchange}")
    logger.info("")

    # Warnings
    if not ccxtpro:
        logger.info("⚠️  Operating in REST-only mode")
        logger.info("✅ System functional with fallback")
    else:
        logger.info("✅ All checks passed")

    return 0


def check_critical_errors() -> List[str]:
    """Check for critical configuration errors that prevent operation."""
    errors = []

    # Check Phase R1 constraint: TRADING_ENABLED must be false
    if hasattr(settings, "current_phase") and settings.current_phase == "R1":
        if settings.trading_enabled:
            errors.append(
                "Configuration error: TRADING_ENABLED is true in Phase R1. "
                "Trading must be disabled during Scanner & Calculations validation."
            )

    # Check if exchanges list is empty
    if not settings.exchanges:
        errors.append("Configuration error: EXCHANGES list is empty")

    return errors


if __name__ == "__main__":
    sys.exit(run())
