"""
Trade cycle logger for МДРК Phase R2+

Thread-safe JSONL logger for recording complete trade cycle history.
Each cycle is written as a single JSON line for easy parsing and analysis.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Optional

from parsertang.trade_models import TradeCycle


logger = logging.getLogger(__name__)


class TradeLogger:
    """
    Thread-safe JSONL logger for trade cycles.

    Writes each completed cycle to a JSONL file where each line is a
    complete JSON object representing one trade cycle.

    Usage:
        logger = TradeLogger("trade_log.jsonl")
        logger.log_cycle(cycle)
    """

    # Class-level lock for thread safety
    _instance_lock = Lock()
    _instances: dict[str, TradeLogger] = {}

    def __init__(
        self, log_file: str = "trade_log.jsonl", base_dir: Optional[str] = None
    ):
        """
        Initialize trade logger.

        Args:
            log_file: Filename for the log file (default: trade_log.jsonl)
            base_dir: Base directory for log file (default: current working directory)
        """
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.log_path = self.base_dir / log_file

        # Instance-level lock for file operations
        self._file_lock = Lock()

        # Ensure log directory exists
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file if it doesn't exist
        if not self.log_path.exists():
            self.log_path.touch()
            logger.info("Created trade log file: %s", self.log_path)
        else:
            logger.info("Using existing trade log file: %s", self.log_path)

    @classmethod
    def get_instance(
        cls, log_file: str = "trade_log.jsonl", base_dir: Optional[str] = None
    ) -> TradeLogger:
        """
        Get or create a singleton instance for a specific log file.

        This ensures we don't create multiple loggers for the same file,
        which could cause file descriptor issues.

        Args:
            log_file: Filename for the log file
            base_dir: Base directory for log file

        Returns:
            TradeLogger instance
        """
        key = f"{base_dir or Path.cwd()}/{log_file}"

        with cls._instance_lock:
            if key not in cls._instances:
                cls._instances[key] = cls(log_file, base_dir)
            return cls._instances[key]

    def log_cycle(self, cycle: TradeCycle) -> None:
        """
        Log a trade cycle to the JSONL file.

        Each cycle is written as a single JSON line with all details.
        The operation is thread-safe.

        Args:
            cycle: TradeCycle to log
        """
        try:
            # Serialize cycle to dict
            cycle_dict = cycle.to_dict()

            # Convert to JSON string
            json_line = json.dumps(
                cycle_dict, ensure_ascii=False, separators=(",", ":")
            )

            # Thread-safe file write
            with self._file_lock:
                with open(self.log_path, "a", encoding="utf-8") as f:
                    f.write(json_line + "\n")
                    f.flush()  # Ensure data is written to disk

            logger.debug(
                "Logged cycle %s (state=%s) to %s",
                cycle.cycle_id,
                cycle.state.value,
                self.log_path.name,
            )

        except Exception as e:
            logger.error("Failed to log cycle %s: %s", cycle.cycle_id, e, exc_info=True)

    def log_cycle_update(self, cycle: TradeCycle, event: str) -> None:
        """
        Log a cycle update/event.

        This is useful for logging intermediate states during cycle execution.
        Creates a snapshot of the current cycle state.

        Args:
            cycle: TradeCycle to log
            event: Description of the update/event
        """
        try:
            cycle_dict = cycle.to_dict()
            cycle_dict["_event"] = event  # Add event marker

            json_line = json.dumps(
                cycle_dict, ensure_ascii=False, separators=(",", ":")
            )

            with self._file_lock:
                with open(self.log_path, "a", encoding="utf-8") as f:
                    f.write(json_line + "\n")
                    f.flush()

            logger.debug("Logged cycle %s update: %s", cycle.cycle_id, event)

        except Exception as e:
            logger.error(
                "Failed to log cycle update %s: %s", cycle.cycle_id, e, exc_info=True
            )

    def count_cycles(self) -> int:
        """
        Count total number of cycles in log file.

        Returns:
            Number of lines (cycles) in log file
        """
        try:
            if not self.log_path.exists():
                return 0

            with open(self.log_path, "r", encoding="utf-8") as f:
                return sum(1 for line in f if line.strip())
        except Exception as e:
            logger.error("Failed to count cycles: %s", e)
            return 0

    def read_cycles(self, limit: Optional[int] = None) -> list[dict]:
        """
        Read cycles from log file.

        Args:
            limit: Maximum number of cycles to read (default: all)

        Returns:
            List of cycle dicts
        """
        cycles = []

        try:
            if not self.log_path.exists():
                return cycles

            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        cycle_dict = json.loads(line)
                        cycles.append(cycle_dict)

                        if limit and len(cycles) >= limit:
                            break
                    except json.JSONDecodeError as e:
                        logger.warning("Failed to parse log line: %s", e)
                        continue

            return cycles

        except Exception as e:
            logger.error("Failed to read cycles: %s", e)
            return cycles

    def get_stats(self) -> dict:
        """
        Get statistics about logged cycles.

        Returns:
            Dict with stats: total, by state, total profit, etc.
        """
        stats = {
            "total_cycles": 0,
            "by_state": {},
            "total_profit_usd": 0.0,
            "successful_cycles": 0,
            "failed_cycles": 0,
        }

        try:
            cycles = self.read_cycles()
            stats["total_cycles"] = len(cycles)

            for cycle in cycles:
                # Count by state
                state = cycle.get("state", "UNKNOWN")
                stats["by_state"][state] = stats["by_state"].get(state, 0) + 1

                # Track success/failure
                if state == "COMPLETE":
                    stats["successful_cycles"] += 1
                    # Add profit
                    results = cycle.get("results", {})
                    profit = results.get("realized_profit_usd", 0.0)
                    stats["total_profit_usd"] += profit
                elif state in ("FAILED", "CANCELLED"):
                    stats["failed_cycles"] += 1

            return stats

        except Exception as e:
            logger.error("Failed to get stats: %s", e)
            return stats

    def rotate_log(self, max_size_mb: int = 100) -> bool:
        """
        Rotate log file if it exceeds max size.

        Renames current log to trade_log.jsonl.1 and creates new file.

        Args:
            max_size_mb: Maximum log file size in MB before rotation

        Returns:
            True if rotation occurred, False otherwise
        """
        try:
            if not self.log_path.exists():
                return False

            # Check file size
            size_mb = self.log_path.stat().st_size / (1024 * 1024)

            if size_mb < max_size_mb:
                return False

            # Rotate
            with self._file_lock:
                # Find next available rotation number
                rotation_num = 1
                while True:
                    rotated_path = Path(f"{self.log_path}.{rotation_num}")
                    if not rotated_path.exists():
                        break
                    rotation_num += 1

                # Rename current log
                self.log_path.rename(rotated_path)

                # Create new log file
                self.log_path.touch()

                logger.info(
                    "Rotated trade log: %s -> %s (size: %.2f MB)",
                    self.log_path.name,
                    rotated_path.name,
                    size_mb,
                )

                return True

        except Exception as e:
            logger.error("Failed to rotate log: %s", e)
            return False

    def __repr__(self) -> str:
        """Human-readable representation."""
        cycle_count = self.count_cycles()
        return f"TradeLogger({self.log_path.name}, cycles={cycle_count})"


# Global default instance
_default_logger: Optional[TradeLogger] = None


def get_default_logger() -> TradeLogger:
    """
    Get the default global trade logger instance.

    Returns:
        TradeLogger instance for default log file
    """
    global _default_logger

    if _default_logger is None:
        _default_logger = TradeLogger.get_instance()

    return _default_logger


def log_cycle(cycle: TradeCycle) -> None:
    """
    Convenience function to log a cycle using the default logger.

    Args:
        cycle: TradeCycle to log
    """
    get_default_logger().log_cycle(cycle)
