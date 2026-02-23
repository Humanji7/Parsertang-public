"""
Logging configuration for Parsertang.

Implements:
- Size-based log rotation (RotatingFileHandler)
- Configurable log levels for console and file
- Sampling filter for noisy log categories (WS, OB, LIQ, TICK)
- Critical prefix bypass (ARB, CYCLE, LEG always logged)
- Sensitive data masking (API keys, tokens)

Configuration via environment variables (see config.py):
- LOG_LEVEL_CONSOLE: Console log level (default: WARNING)
- LOG_LEVEL_FILE: File log level (default: INFO)
- LOG_MAX_BYTES: Max file size before rotation (default: 100MB)
- LOG_BACKUP_COUNT: Number of backup files (default: 5)
- LOG_SAMPLE_RATIO: Sampling ratio for noisy logs (default: 10)
- LOG_SAMPLE_INTERVAL_SECONDS: Time gate for sampling (default: 0)
- LOG_SUPPRESS_PREFIXES: Prefixes to suppress entirely (default: "")
"""

from __future__ import annotations

import logging
import logging.handlers
import re
import sys
import time
from pathlib import Path
from typing import Set


class SensitiveDataFilter(logging.Filter):
    """
    Filter to mask sensitive data in log messages.

    Masks:
    - WebSocket tokens in URLs (KuCoin, etc.)
    - API keys/secrets if accidentally logged
    """

    PATTERNS = [
        # KuCoin WS token: ?token=XXXXX
        (r"(\?token=)([A-Za-z0-9_-]+)", r"\1***MASKED***"),
        # Generic API key patterns
        (r"(api[_-]?key[=:]\s*)([A-Za-z0-9_-]{20,})", r"\1***MASKED***"),
        (r"(secret[=:]\s*)([A-Za-z0-9_-]{20,})", r"\1***MASKED***"),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """Mask sensitive data in log message."""
        # Guard against shutdown race condition
        if sys.meta_path is None:
            return True

        try:
            if hasattr(record, "msg") and isinstance(record.msg, str):
                for pattern, replacement in self.PATTERNS:
                    record.msg = re.sub(pattern, replacement, record.msg)

            if hasattr(record, "args") and record.args:
                new_args = []
                for arg in record.args:
                    if isinstance(arg, str):
                        for pattern, replacement in self.PATTERNS:
                            arg = re.sub(pattern, replacement, arg)
                    new_args.append(arg)
                record.args = tuple(new_args)
        except (ImportError, TypeError):
            # Python shutting down, skip masking
            pass

        return True


class SamplingFilter(logging.Filter):
    """
    Filter that samples noisy log categories while allowing critical prefixes through.

    Args:
        ratio: Sample 1 out of every N messages (1 = no sampling, 10 = 1 in 10)
        interval: Optional time gate - max 1 message per interval seconds per prefix
        suppress: Set of prefixes to suppress entirely
    """

    CRITICAL_PREFIXES: Set[str] = {"ARB", "CYCLE", "LEG", "ERROR", "CRITICAL"}
    ALWAYS_ALLOW_STARTS: tuple[str, ...] = (
        # Startup diagnostics / allocation visibility
        "BUILD |",
        "MARKETS |",
        "SYMBOLS |",
        # WS observability (keep these visible even when WS prefix is sampled)
        "WS INIT |",
        "WS INIT FAILED |",
        "WS HEALTH |",
        "WS LEGACY |",
        "WS SKIP |",
        "V2 VALIDATION SUMMARY |",
        "V2 VALIDATION |",
        "WSNATIVE HEALTH |",
        "WSNATIVE STALE |",
        "WSNATIVE |",
        "WSNATIVE BYBIT |",
        "WSNATIVE BYBIT | data_sample",
        "FEE SNAPSHOT |",
        "FEE VALIDATION |",
        "FEE VALIDATION SUMMARY |",
        "SIGNAL SNAPSHOT |",
        "TRUTH OK |",
        "TRUTH FAIL |",
        "TRUTH SUMMARY |",
        "ALERTTRUTH OK |",
        "ALERTTRUTH FAIL |",
        "WS HYBRID |",
        "ALERT SENT |",
        "ALERT SUPPRESSED |",
        "ALERT ERROR |",
        "ALERT FUNNEL |",
        # REST snapshot observability
        "REST SNAPSHOT |",
    )

    def __init__(
        self,
        ratio: int = 10,
        interval: float = 0.0,
        suppress: Set[str] | None = None,
    ):
        super().__init__()
        self.ratio = max(1, ratio)  # Prevent division by zero
        self.interval = max(0.0, interval)
        self.suppress = suppress or set()
        self.counters: dict[str, int] = {}
        self.last_logged: dict[str, float] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Determine if a log record should be emitted.

        Returns:
            True if the record should be logged, False otherwise
        """
        # Guard against shutdown race condition
        if sys.meta_path is None:
            return True

        try:
            # Extract prefix from message (first word before space)
            msg = record.getMessage()
        except (ImportError, TypeError):
            return True  # Python shutting down

        if not msg:
            return True  # Always log empty messages

        # Always allow key observability lines regardless of sampling ratio.
        # These are low-frequency but essential for diagnosing startup/WS issues.
        if msg.startswith(self.ALWAYS_ALLOW_STARTS):
            return True

        prefix = msg.split()[0] if msg else ""

        # Suppression list: completely filter these prefixes
        if prefix in self.suppress:
            return False

        # Critical prefixes bypass all sampling
        if any(prefix.startswith(crit) for crit in self.CRITICAL_PREFIXES):
            return True

        # Also bypass sampling for WARNING and above
        if record.levelno >= logging.WARNING:
            return True

        # Counter-based sampling for noisy categories
        if self.ratio > 1:
            self.counters[prefix] = self.counters.get(prefix, 0) + 1
            if self.counters[prefix] % self.ratio != 0:
                return False

        # Optional time-based interval gate
        if self.interval > 0:
            now = time.time()
            last = self.last_logged.get(prefix, 0.0)
            if now - last < self.interval:
                return False
            self.last_logged[prefix] = now

        return True


def setup_logging() -> None:
    """
    Configure application logging with rotation, sampling, and configurable levels.

    Reads configuration from environment variables via Settings.
    Falls back to console-only logging if log file is not writable.
    """
    # Import here to avoid circular imports
    from parsertang.config import settings

    # Parse log levels (already validated and uppercase from Settings)
    console_level = getattr(logging, settings.log_level_console)
    file_level = getattr(logging, settings.log_level_file)

    # Root logger configuration
    root_logger = logging.getLogger()

    # Skip if already configured (prevent duplicate handlers)
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.DEBUG)  # Capture everything, filter at handlers

    # Log format with structured prefixes
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    # Create filters
    sensitive_filter = SensitiveDataFilter()
    suppress_prefixes = set(settings.get_suppress_prefixes())
    sampling_filter = SamplingFilter(
        ratio=settings.log_sample_ratio,
        interval=settings.log_sample_interval_seconds,
        suppress=suppress_prefixes,
    )

    # --- File Handler with Rotation ---
    log_file = Path("parsertang.log")
    file_handler_added = False

    try:
        log_file.touch(exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=settings.log_max_bytes,
            backupCount=settings.log_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(sensitive_filter)
        file_handler.addFilter(sampling_filter)
        root_logger.addHandler(file_handler)
        file_handler_added = True
    except (PermissionError, OSError) as e:
        print(f"WARNING: Cannot write to {log_file}: {e}", file=sys.stderr)
        print("Logging to console only", file=sys.stderr)

    # --- Console Handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(sensitive_filter)
    console_handler.addFilter(sampling_filter)
    root_logger.addHandler(console_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("ccxt").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Log active configuration at startup
    if file_handler_added:
        root_logger.info(
            "Logging configured: console=%s file=%s rotation=%dMB×%d sample=%d interval=%.1fs",
            settings.log_level_console,
            settings.log_level_file,
            settings.log_max_bytes // 1048576,  # Convert to MB
            settings.log_backup_count,
            settings.log_sample_ratio,
            settings.log_sample_interval_seconds,
        )
    else:
        root_logger.info(
            "Logging configured: console=%s (file disabled) sample=%d",
            settings.log_level_console,
            settings.log_sample_ratio,
        )

    if suppress_prefixes:
        root_logger.info(
            "Suppressing prefixes: %s", ", ".join(sorted(suppress_prefixes))
        )
