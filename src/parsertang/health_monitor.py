"""Circuit Breaker pattern implementation for exchange API protection.

This module provides fault tolerance for ExchangeGateway by implementing
the Circuit Breaker pattern with three states: CLOSED, OPEN, HALF_OPEN.

Key features:
- Per-exchange isolation (one circuit per exchange)
- threading.Lock for thread-safe state updates
- Failure classification (only count network/timeout errors)
- Configurable thresholds via Settings
"""

from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from parsertang.v2.sla_report import format_sla_report

if TYPE_CHECKING:
    from parsertang.config import Settings

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Three states of the Circuit Breaker."""

    CLOSED = "closed"  # Normal operation, counting failures
    OPEN = "open"  # Rejecting all calls, waiting for recovery timeout
    HALF_OPEN = "half_open"  # Testing recovery with limited probe calls


# Exceptions that should trigger circuit breaker (network/timeout issues)
CIRCUIT_TRIGGERING_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,  # Includes network unreachable, etc.
)

# Exception message patterns that indicate transient failures
TRANSIENT_ERROR_PATTERNS = (
    "timeout",
    "timed out",
    "connection refused",
    "connection reset",
    "network unreachable",
    "temporary failure",
    "service unavailable",
    "502",
    "503",
    "504",
    "rate limit",
    "too many requests",
    "429",
)


def is_transient_failure(error: Exception) -> bool:
    """Determine if an error should increment the circuit failure counter.

    Only network issues, timeouts, and server errors (5xx) should trigger
    the circuit breaker. Client errors (4xx) and business logic errors
    should NOT trigger it.

    Args:
        error: The exception to classify

    Returns:
        True if this is a transient failure that should increment counter
    """
    # Direct match on exception type
    if isinstance(error, CIRCUIT_TRIGGERING_EXCEPTIONS):
        return True

    # Check error message for known patterns
    error_str = str(error).lower()
    for pattern in TRANSIENT_ERROR_PATTERNS:
        if pattern in error_str:
            return True

    return False


class CircuitOpenError(Exception):
    """Raised when circuit is open and call is rejected."""

    def __init__(self, exchange_id: str, retry_after_seconds: int):
        self.exchange_id = exchange_id
        self.retry_after = retry_after_seconds
        super().__init__(
            f"Circuit OPEN for {exchange_id}. Retry after {retry_after_seconds}s"
        )


@dataclass
class ExchangeCircuit:
    """Per-exchange circuit breaker state."""

    exchange_id: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: datetime | None = None
    last_state_change: datetime = field(default_factory=datetime.utcnow)
    last_error_type: str | None = None

    # Half-open probe tracking
    probe_in_progress: bool = False

    # Config (injected from Settings)
    failure_threshold: int = 5
    recovery_timeout_seconds: int = 300  # 5 minutes


class ExchangeHealthMonitor:
    """Circuit Breaker manager for all exchange connections.

    Provides fault tolerance by tracking failures per exchange and
    automatically stopping requests to unhealthy exchanges.

    Thread-safety: Uses threading.Lock per exchange for safe concurrent access.

    Usage:
        monitor = ExchangeHealthMonitor(settings)

        # Check before calling
        if monitor.is_available("bybit"):
            try:
                result = some_api_call()
                monitor.record_success("bybit")
            except Exception as e:
                monitor.record_failure("bybit", e)
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize with configuration from Settings.

        Args:
            settings: Application settings containing circuit breaker config
        """
        self._settings = settings
        self._circuits: dict[str, ExchangeCircuit] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._master_lock = threading.Lock()  # For creating new circuits/locks

    def _get_or_create_circuit(self, exchange_id: str) -> ExchangeCircuit:
        """Get existing circuit or create new one with default config."""
        if exchange_id not in self._circuits:
            with self._master_lock:
                if exchange_id not in self._circuits:  # Double-check
                    self._circuits[exchange_id] = ExchangeCircuit(
                        exchange_id=exchange_id,
                        failure_threshold=self._settings.circuit_failure_threshold,
                        recovery_timeout_seconds=self._settings.circuit_recovery_timeout_seconds,
                    )
        return self._circuits[exchange_id]

    def _get_lock(self, exchange_id: str) -> threading.Lock:
        """Get or create lock for exchange."""
        if exchange_id not in self._locks:
            with self._master_lock:
                if exchange_id not in self._locks:  # Double-check
                    self._locks[exchange_id] = threading.Lock()
        return self._locks[exchange_id]

    def get_state(self, exchange_id: str) -> CircuitState:
        """Get current circuit state for an exchange."""
        circuit = self._get_or_create_circuit(exchange_id)
        return circuit.state

    def is_available(self, exchange_id: str) -> bool:
        """Check if exchange is available for requests.

        Returns True if:
        - Circuit is CLOSED (normal operation)
        - Circuit is OPEN but recovery timeout has elapsed (will transition to HALF_OPEN)
        - Circuit is HALF_OPEN and no probe is in progress

        Returns False if:
        - Circuit is OPEN and recovery timeout has NOT elapsed
        - Circuit is HALF_OPEN and a probe is already in progress
        """
        if not self._settings.circuit_breaker_enabled:
            return True

        circuit = self._get_or_create_circuit(exchange_id)

        if circuit.state == CircuitState.CLOSED:
            return True

        if circuit.state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            elapsed = datetime.utcnow() - circuit.last_state_change
            if elapsed >= timedelta(seconds=circuit.recovery_timeout_seconds):
                return True  # Will transition to HALF_OPEN on next call
            return False

        if circuit.state == CircuitState.HALF_OPEN:
            # Only allow if no probe is in progress
            return not circuit.probe_in_progress

        return False

    def record_success(self, exchange_id: str) -> None:
        """Record successful API call.

        Resets failure counter and transitions:
        - HALF_OPEN → CLOSED (probe succeeded)
        - CLOSED → stays CLOSED (reset counter)
        """
        if not self._settings.circuit_breaker_enabled:
            return

        with self._get_lock(exchange_id):
            circuit = self._get_or_create_circuit(exchange_id)

            if circuit.state == CircuitState.HALF_OPEN:
                self._transition_state(circuit, CircuitState.CLOSED, "probe_success")
                circuit.probe_in_progress = False

            # Reset failure counter on any success
            circuit.failure_count = 0
            circuit.last_error_type = None

    def record_failure(self, exchange_id: str, error: Exception) -> None:
        """Record failed API call.

        Only counts transient failures (network, timeout, 5xx).
        Increments counter and may transition CLOSED → OPEN.
        """
        if not self._settings.circuit_breaker_enabled:
            return

        # Only count transient failures
        if not is_transient_failure(error):
            logger.debug(
                "CIRCUIT_IGNORE | exchange=%s error_type=%s (not transient)",
                exchange_id,
                type(error).__name__,
            )
            return

        with self._get_lock(exchange_id):
            circuit = self._get_or_create_circuit(exchange_id)
            circuit.failure_count += 1
            circuit.last_failure_time = datetime.utcnow()
            circuit.last_error_type = type(error).__name__

            if circuit.state == CircuitState.HALF_OPEN:
                # Probe failed, reopen circuit
                self._transition_state(circuit, CircuitState.OPEN, "probe_failed")
                circuit.probe_in_progress = False

            elif circuit.state == CircuitState.CLOSED:
                # Check threshold
                if circuit.failure_count >= circuit.failure_threshold:
                    self._transition_state(
                        circuit,
                        CircuitState.OPEN,
                        f"threshold_reached ({circuit.failure_count}/{circuit.failure_threshold})",
                    )

    def acquire_probe(self, exchange_id: str) -> bool:
        """Try to acquire probe slot for HALF_OPEN state.

        Returns True if this call should proceed as a probe.
        Returns False if another probe is already in progress.

        Must be called before making a request when circuit may be OPEN.
        """
        if not self._settings.circuit_breaker_enabled:
            return True

        with self._get_lock(exchange_id):
            circuit = self._get_or_create_circuit(exchange_id)

            if circuit.state == CircuitState.OPEN:
                # Check if we should transition to HALF_OPEN
                elapsed = datetime.utcnow() - circuit.last_state_change
                if elapsed >= timedelta(seconds=circuit.recovery_timeout_seconds):
                    self._transition_state(
                        circuit, CircuitState.HALF_OPEN, "recovery_timeout_elapsed"
                    )
                    circuit.probe_in_progress = True
                    return True
                return False

            if circuit.state == CircuitState.HALF_OPEN:
                if circuit.probe_in_progress:
                    return False  # Another probe in progress
                circuit.probe_in_progress = True
                return True

            # CLOSED state - always allow
            return True

    def get_retry_after(self, exchange_id: str) -> int:
        """Get seconds until circuit might transition to HALF_OPEN."""
        circuit = self._get_or_create_circuit(exchange_id)
        if circuit.state != CircuitState.OPEN:
            return 0

        elapsed = datetime.utcnow() - circuit.last_state_change
        remaining = circuit.recovery_timeout_seconds - elapsed.total_seconds()
        return max(0, int(remaining))

    def get_health_summary(self) -> dict[str, dict[str, Any]]:
        """Return health status for all exchanges.

        Returns:
            Dict with exchange_id as key and status dict as value.
            Useful for monitoring dashboards and Telegram alerts.
        """
        summary = {}
        for exchange_id, circuit in self._circuits.items():
            retry_after = self.get_retry_after(exchange_id)
            summary[exchange_id] = {
                "state": circuit.state.value,
                "failure_count": circuit.failure_count,
                "last_failure_time": (
                    circuit.last_failure_time.isoformat()
                    if circuit.last_failure_time
                    else None
                ),
                "last_error_type": circuit.last_error_type,
                "retry_after_seconds": retry_after,
                "probe_in_progress": circuit.probe_in_progress,
            }
        return summary

    def _transition_state(
        self, circuit: ExchangeCircuit, new_state: CircuitState, reason: str
    ) -> None:
        """Internal: Change state with defensive logging.

        Logs at WARNING level for OPEN transitions, INFO for others.
        """
        old_state = circuit.state
        circuit.state = new_state
        circuit.last_state_change = datetime.utcnow()

        # Reset failure count when closing
        if new_state == CircuitState.CLOSED:
            circuit.failure_count = 0

        # Defensive logging
        log_level = logging.WARNING if new_state == CircuitState.OPEN else logging.INFO
        logger.log(
            log_level,
            "CIRCUIT_%s | exchange=%s prev=%s reason=%s failures=%d",
            new_state.value.upper(),
            circuit.exchange_id,
            old_state.value,
            reason,
            circuit.failure_count,
        )


async def v2_sla_report_task(
    *,
    get_metrics_fn: Callable[[], tuple[float, float]],
    send_fn: Callable[[str], Coroutine[Any, Any, Any]],
    interval_seconds: int = 60 * 60 * 24,
    run_once: bool = False,
) -> None:
    """Background task that sends daily V2 SLA report to tech chat."""
    logger.info("V2 SLA report task started (interval=%ds)", interval_seconds)

    while True:
        try:
            await asyncio.sleep(interval_seconds)
            healthy_ratio, fresh_ratio_min = get_metrics_fn()
            report = format_sla_report(
                healthy_ratio=healthy_ratio, fresh_ratio_min=fresh_ratio_min
            )
            await send_fn(report)
            logger.info(
                "V2 SLA report sent (healthy_ratio=%.2f fresh_ratio_min=%.2f)",
                healthy_ratio,
                fresh_ratio_min,
            )

            if run_once:
                return

        except asyncio.CancelledError:
            logger.info("V2 SLA report task cancelled")
            raise

        except Exception as e:
            logger.error("V2 SLA report task error: %s", e)
            if run_once:
                raise
