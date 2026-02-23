import json
import logging
from typing import List, Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_EXCHANGES = [
    "bybit",
    "okx",
    "kucoin",
    "mexc",
    "htx",
    "gate",
]

WS_ID_ALIASES: dict[str, str] = {
    "binance": "binance",
    "bybit": "bybit",
    "kucoin": "kucoin",
    "mexc": "mexc",
    "htx": "huobi",
    "gate": "gate",
    "gateio": "gate",
}

SUPPORTED_ORDERBOOK_LIMITS: dict[str, list[int]] = {
    "bybit": [1, 50, 200, 1000],
    "kucoin": [20, 100],  # KuCoin only supports 20 or 100
    "okx": [5],  # Only books5 channel (no VIP required, VIP4+ needed for 50+)
    "gate": [5, 10, 20, 50, 100],
    "mexc": [5, 10, 20, 50, 100, 500, 1000],
    "htx": [5, 20, 150, 400],  # HTX only supports these specific limits
}

# Exchanges excluded from batch WS subscriptions (watchOrderBookForSymbols)
# These use per-symbol subscriptions as fallback
BATCH_EXCLUDED_EXCHANGES: frozenset[str] = frozenset(
    {
        "okx",  # Requires special auth configuration for batch API
        "gate",  # ccxt client issues with batch connections
        "bybit",  # Batch API limited to 10 symbols, per-symbol works at 30
        "htx",  # Batch WS not appearing in WS HEALTH, per-symbol more stable
        "kucoin",  # Batch WS exceeds 100 subscription limit, per-symbol required
    }
)

# Per-exchange symbol limits (overrides MAX_SYMBOLS_PER_EXCHANGE)
# Used for exchanges with connection constraints (e.g., SOCKS5 proxy limits)
EXCHANGE_SYMBOL_LIMITS: dict[str, int] = {
    "htx": 15,  # Residential SOCKS5 can't handle 70 concurrent WS connections
    # Gate WS shows intermittent ping/pong keepalive timeouts under high fan-out.
    # Keep a conservative default to improve WS stability.
    "gate": 10,
    # MEXC is prone to ping/pong keepalive timeouts under high fan-out.
    "mexc": 20,
}


def get_exchange_symbol_limits() -> dict[str, int]:
    """Return effective per-exchange symbol limits.

    Defaults come from EXCHANGE_SYMBOL_LIMITS but can be overridden via
    EXCHANGE_SYMBOL_LIMITS_JSON in .env, for example:
      EXCHANGE_SYMBOL_LIMITS_JSON={"gate": 50, "mexc": 80}
    """
    limits = dict(EXCHANGE_SYMBOL_LIMITS)
    raw = getattr(settings, "exchange_symbol_limits_json", None)
    if not raw:
        return limits
    try:
        parsed = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"Invalid EXCHANGE_SYMBOL_LIMITS_JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise ValueError("EXCHANGE_SYMBOL_LIMITS_JSON must be a JSON object")
    for ex_id, value in parsed.items():
        if not isinstance(ex_id, str):
            raise ValueError("EXCHANGE_SYMBOL_LIMITS_JSON keys must be strings")
        try:
            limit_int = int(value)
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] must be int"
            ) from e
        if limit_int < 0:
            raise ValueError(f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] must be >= 0")
        if limit_int > 5000:
            raise ValueError(
                f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] too large (>5000)"
            )
        limits[ex_id] = limit_int
    return limits


# HARDCODED: Never trade these assets - expensive fees, outside trading scope
# This is a business rule, NOT configurable via .env
# XRP - requires destination tag (complex withdrawal)
# BNB - Binance-native, withdrawal restrictions
# TRX - we use TRC20 network for USDT, TRX itself has low arbitrage potential
EXCLUDED_BASE_ASSETS = frozenset({"BTC", "ETH", "SOL", "XRP", "BNB", "TRX"})


class Settings(BaseSettings):
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    telegram_tech_chat_id: str | None = None  # Technical channel for daily reports
    telegram_send_suppressed_alerts_to_tech: bool = True
    access_control_ids: str | None = None  # Multi-user access control (JSON or CSV)

    # Daily Fee Report Configuration
    enable_daily_fee_report: bool = False
    exchanges: List[str] = DEFAULT_EXCHANGES
    ws_exchanges: List[str] | None = None
    exchanges_source: str | None = None
    min_net_profit: float = 0.5
    # Optional higher threshold for "trader" channel alerts.
    # If set, opportunities with net < min_net_profit_trade are routed to tech channel
    # (still subject to the base min_net_profit threshold + truth/fee validation).
    min_net_profit_trade: float | None = None
    profit_mode: Literal["transfer", "funded"] = "transfer"
    trade_volume_usd: float = 100.0
    liquidity_usd_threshold: float = 10_000.0
    liquidity_window_pct: float = 0.1

    # Smart Liquidity & Slippage (CEX execution guard)
    # Reject opportunities where expected slippage would consume too much of net profit.
    slippage_budget_fraction: float = 0.25
    orderbook_stale_seconds: float = 2.0
    ws_enabled: bool = True
    ws_native_enabled: bool = False
    ws_native_exchanges: List[str] = ["bybit", "okx", "mexc"]
    ws_native_depth_refresh_seconds: int = 5
    ws_native_depth_ttl_seconds: int = 15
    ws_native_bbo_channel: str = "bbo"
    rest_fallback: bool = True
    rest_snapshot_enabled: bool = False
    rest_snapshot_exchanges: List[str] = ["kucoin"]
    rest_snapshot_max_symbols: int = 30
    rest_snapshot_interval_seconds: int = 30
    rest_snapshot_log_interval_seconds: int = 300
    rest_snapshot_restart_enabled: bool = True
    rest_snapshot_restart_min_samples: int = 20
    rest_snapshot_restart_err_rate_threshold: float = 0.7
    rest_snapshot_restart_min_ok: int = 1
    rest_snapshot_restart_cooldown_seconds: int = 300
    check_interval_seconds: int = 5
    run_duration_seconds: int | None = None
    max_symbols_per_exchange: int = 30
    exchange_symbol_limits_json: str | None = None
    symbol_selection_strategy: Literal[
        "local_volume",
        "cross_exchange",
        "core_periphery",
    ] = "cross_exchange"
    core_exchanges: List[str] = ["bybit", "okx", "kucoin"]
    periphery_exchanges: List[str] = ["gate", "mexc"]
    orderbook_limit: int = 50
    network_default: str = "TRC20"
    allowed_networks: list[str] = []
    fees_static_json: str | None = None
    symbol_min_quote_volume_usd: float = 0.0
    symbol_min_overlap_exchanges: int = 2
    symbol_diversify_fraction: float = 0.0
    symbol_diversify_pool_multiplier: int = 5
    symbol_allowlist: List[str] | None = None
    symbol_allowlist_path: str | None = None
    symbol_allowlist_refresh_seconds: int = 3600

    # МДРК Trading Configuration (Phase R2+)
    trading_enabled: bool = False
    dry_run_mode: bool = True
    max_concurrent_cycles: int = 1
    leg1_timeout_seconds: int = 5
    leg3_timeout_seconds: int = 10
    max_position_size_usd: float = 100.0

    # Phase R3: LEG2 confirmation
    leg2_confirmation_timeout_seconds: int = 300  # 5 minutes default

    # Logging Configuration (Protocol 0001)
    log_level_console: str = "WARNING"
    log_level_file: str = "INFO"
    log_max_bytes: int = 104857600  # 100 MB
    log_backup_count: int = 5
    log_sample_ratio: int = 10
    log_sample_interval_seconds: float = 0.0
    log_suppress_prefixes: str = ""  # Comma-separated list

    # Alert trace instrumentation (env-guarded, rate-limited)
    alert_trace_enabled: bool = False
    alert_trace_stale_seconds: float = 2.0
    alert_trace_symbols: str | None = None
    alert_trace_interval_seconds: int = 10

    # Alert evidence + post-facto verification (ALERTTRUTH)
    alert_evidence_enabled: bool = False
    alert_evidence_path: str = "data/alert_evidence.jsonl"
    alert_verify_enabled: bool = False
    alert_verify_delay_seconds: float = 0.5
    alert_verify_fee_enabled: bool = True
    alert_verify_fee_tolerance_pct: float = 2.0
    alert_verify_fee_tolerance_base: float = 0.0

    # Live fee validation (slow-path, blocks alert send)
    fee_live_validation_enabled: bool = False
    fee_live_validation_tolerance_pct: float = 2.0
    fee_live_validation_tolerance_base: float = 0.0

    # Fee debug instrumentation (env-guarded, rate-limited)
    # Examples:
    #   DEBUG_FEE_SYMBOLS=APT/USDT,ARB/USDT
    #   DEBUG_FEE_SYMBOLS=*
    debug_fee_symbols: str | None = None
    debug_fee_log_interval_seconds: int = 60
    debug_fee_only_on_error: bool = True

    # Adaptive symbol refresh (dynamic universe)
    symbol_refresh_enabled: bool = True
    symbol_refresh_check_interval_seconds: int = 300
    symbol_refresh_min_interval_seconds: int = 3600
    symbol_refresh_stale_intervals_threshold: int = 5
    symbol_refresh_stale_exchanges_threshold: int = 1
    symbol_refresh_min_arb_ok: int = 1
    symbol_refresh_min_arb_reject: int = 200

    # Adaptive symbol ramp (health-gated growth)
    symbol_ramp_enabled: bool = False
    symbol_ramp_check_interval_seconds: int = 60
    symbol_ramp_window_seconds: int = 300
    symbol_ramp_min_interval_seconds: int = 300
    symbol_ramp_min_multi_ex_symbols: int = 55
    symbol_ramp_max_stale_exchanges: int = 1
    symbol_ramp_step_core: int = 2
    symbol_ramp_step_periphery: int = 2
    symbol_ramp_max_increase: int = 10
    symbol_ramp_max_limits_json: str | None = None

    # V2 shadow pipeline logging
    v2_shadow_log_level: str = "DEBUG"  # DEBUG/INFO/WARN/ERROR/OFF
    v2_health_enabled: bool = True
    v2_health_fresh_ratio_min: float = 0.80
    v2_health_stale_seconds: float = 2.0
    v2_health_check_interval_seconds: int = 60
    v2_validation_enabled: bool = False
    v2_validation_price_tolerance_pct: float = 0.1
    v2_validation_tick_multiplier: int = 3
    v2_validation_ws_max_age_ms: int = 1000
    v2_validation_ws_max_skew_ms: int = 500
    v2_validation_fee_max_age_seconds: int = 3600
    v2_validation_stale_symbol_threshold: int = 5
    v2_validation_stale_symbol_cooldown_seconds: int = 600
    v2_validation_rest_symbol_threshold: int = 10
    v2_validation_rest_symbol_cooldown_seconds: int = 1800
    v2_truth_probe_enabled: bool = False
    v2_truth_probe_interval_seconds: float = 30.0
    v2_truth_probe_summary_interval_seconds: float = 3600.0
    v2_truth_probe_tech_summary_enabled: bool = False
    v2_truth_fail_tech_alert_enabled: bool = False
    v2_truth_fail_tech_alert_interval_seconds: float = 300.0
    truth_gate_enabled: bool = True
    truth_gate_ratio_min: float = 98.0
    truth_gate_summary_path: str = "data/truth_summary.json"
    truth_gate_max_age_seconds: int = 3600
    truth_gate_refresh_seconds: float = 30.0
    truth_gate_min_total: int = 500
    truth_allowlist_path: str | None = None
    truth_allowlist_refresh_seconds: float = 30.0

    # WS Guard (no-overlap watchdog)
    ws_guard_enabled: bool = True
    ws_guard_check_interval_seconds: int = 60
    ws_guard_no_overlap_minutes: int = 1
    ws_guard_restart_min_interval_minutes: int = 30
    ws_guard_min_active_exchanges: int = 3
    ws_guard_stale_exchanges_threshold: int = 2
    ws_guard_state_path: str = "ws_guard_state.json"
    ws_guard_log_path: str = "parsertang.log"

    # WS Guard (no-overlap watchdog)
    ws_guard_enabled: bool = True
    ws_guard_check_interval_seconds: int = 60
    ws_guard_no_overlap_minutes: int = 1
    ws_guard_restart_min_interval_minutes: int = 30
    ws_guard_min_active_exchanges: int = 3
    ws_guard_stale_exchanges_threshold: int = 2
    ws_guard_state_path: str = "ws_guard_state.json"
    ws_guard_log_path: str = "parsertang.log"

    # Proxy configuration (for geographic restrictions)
    http_proxy: str | None = None
    https_proxy: str | None = None

    # Dynamic Withdrawal Fees Configuration (SPEC-FEE-001)
    withdrawal_fee_cache_lifetime: int = 3600  # 1 hour in seconds
    withdrawal_fee_fetch_timeout: int = 10  # API fetch timeout in seconds
    use_dynamic_withdrawal_fees: bool = True  # Feature flag for gradual rollout
    require_high_fee_confidence: bool = (
        False  # If True, reject LOW confidence opportunities
    )

    # Circuit Breaker Configuration (Phase 3 - Reliability)
    circuit_breaker_enabled: bool = True
    circuit_failure_threshold: int = 5  # Consecutive failures before OPEN
    circuit_recovery_timeout_seconds: int = 300  # 5 min before HALF_OPEN probe
    circuit_half_open_max_calls: int = 1  # Probe calls allowed in HALF_OPEN

    # Alert Deduplication Configuration
    alert_dedup_threshold_pct: float = (
        0.1  # Resend alert if net_profit changed by >0.1%
    )

    # Phase tracking (internal)
    current_phase: str = "R2"  # Updated to R2 (completed), moving to R3

    # Exchange API credentials (for dynamic withdrawal fees)
    bybit_api_key: str | None = None
    bybit_secret: str | None = None
    okx_api_key: str | None = None
    okx_secret: str | None = None
    okx_password: str | None = None
    kucoin_api_key: str | None = None
    kucoin_secret: str | None = None
    kucoin_password: str | None = None
    htx_api_key: str | None = None
    htx_secret: str | None = None
    htx_proxy: str | None = None  # Poland proxy for Singapore geo-block bypass
    gate_api_key: str | None = None
    gate_secret: str | None = None
    mexc_api_key: str | None = None
    mexc_secret: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
    )

    _exchanges_source_tracker: str | None = None

    @field_validator("trade_volume_usd")
    @classmethod
    def validate_trade_volume(cls, v):
        """Validate trade_volume_usd is positive (Issue 2.2 - P2)."""
        if v <= 0:
            raise ValueError("trade_volume_usd must be positive")
        if v < 1.0:
            logging.getLogger(__name__).warning(
                f"trade_volume_usd is very small: ${v}. "
                "This may cause precision issues in profit calculations."
            )
        return v

    @field_validator("v2_validation_price_tolerance_pct")
    @classmethod
    def clamp_validation_tolerance(cls, v):
        if v < 0:
            logging.getLogger(__name__).warning(
                "v2_validation_price_tolerance_pct is negative (%.3f); clamping to 0.0",
                v,
            )
            return 0.0
        return v

    @field_validator("v2_validation_tick_multiplier")
    @classmethod
    def validate_tick_multiplier(cls, v):
        if v < 1:
            raise ValueError("v2_validation_tick_multiplier must be >= 1")
        return v

    @field_validator("symbol_min_quote_volume_usd")
    @classmethod
    def validate_min_quote_volume(cls, v):
        if v < 0:
            raise ValueError("symbol_min_quote_volume_usd must be >= 0")
        return v

    @field_validator("symbol_min_overlap_exchanges")
    @classmethod
    def validate_min_overlap(cls, v):
        if v < 2:
            raise ValueError("symbol_min_overlap_exchanges must be >= 2")
        return v

    @field_validator("symbol_diversify_fraction")
    @classmethod
    def validate_symbol_diversify_fraction(cls, v):
        if v < 0:
            raise ValueError("symbol_diversify_fraction must be >= 0")
        if v > 0.95:
            logging.getLogger(__name__).warning(
                "symbol_diversify_fraction is very high (%.3f); clamping to 0.95",
                v,
            )
            return 0.95
        return v

    @field_validator("symbol_diversify_pool_multiplier")
    @classmethod
    def validate_symbol_diversify_pool_multiplier(cls, v):
        if v < 1:
            raise ValueError("symbol_diversify_pool_multiplier must be >= 1")
        return v

    @field_validator("symbol_allowlist", mode="before")
    @classmethod
    def parse_symbol_allowlist(cls, v):
        if v is None:
            return None
        if isinstance(v, list):
            return [item.strip().upper() for item in v if str(item).strip()]
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return None
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError("SYMBOL_ALLOWLIST must be valid JSON list") from e
                if not isinstance(parsed, list):
                    raise ValueError("SYMBOL_ALLOWLIST must be a JSON list")
                return [str(item).strip().upper() for item in parsed if str(item).strip()]
            parts = [item.strip().upper() for item in stripped.split(",") if item.strip()]
            return parts or None
        return v

    @field_validator("allowed_networks", mode="before")
    @classmethod
    def parse_allowed_networks(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return [str(item).strip().upper() for item in v if str(item).strip()]
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError("ALLOWED_NETWORKS must be valid JSON list") from e
                if not isinstance(parsed, list):
                    raise ValueError("ALLOWED_NETWORKS must be a JSON list")
                return [str(item).strip().upper() for item in parsed if str(item).strip()]
            return [item.strip().upper() for item in stripped.split(",") if item.strip()]
        return v

    @field_validator("symbol_allowlist_refresh_seconds")
    @classmethod
    def validate_allowlist_refresh_seconds(cls, v):
        if v < 0:
            raise ValueError("symbol_allowlist_refresh_seconds must be >= 0")
        return v

    @field_validator("v2_validation_ws_max_age_ms", "v2_validation_ws_max_skew_ms")
    @classmethod
    def validate_ws_thresholds(cls, v):
        if v < 0:
            raise ValueError("v2_validation_ws thresholds must be >= 0")
        return v

    @field_validator("v2_validation_fee_max_age_seconds")
    @classmethod
    def validate_fee_age(cls, v):
        if v < 0:
            raise ValueError("v2_validation_fee_max_age_seconds must be >= 0")
        return v

    @field_validator("v2_truth_probe_interval_seconds")
    @classmethod
    def validate_truth_probe_interval(cls, v):
        if v < 0:
            raise ValueError("v2_truth_probe_interval_seconds must be >= 0")
        return v

    @field_validator(
        "v2_truth_probe_summary_interval_seconds",
        "v2_truth_fail_tech_alert_interval_seconds",
    )
    @classmethod
    def validate_truth_probe_intervals(cls, v):
        if v < 0:
            raise ValueError("v2_truth_probe intervals must be >= 0")
        return v

    @field_validator("truth_gate_ratio_min")
    @classmethod
    def validate_truth_gate_ratio(cls, v):
        if v < 0 or v > 100:
            raise ValueError("truth_gate_ratio_min must be between 0 and 100")
        return v

    @field_validator(
        "truth_gate_max_age_seconds",
        "truth_gate_refresh_seconds",
        "truth_allowlist_refresh_seconds",
    )
    @classmethod
    def validate_truth_gate_intervals(cls, v):
        if v < 0:
            raise ValueError("truth_gate intervals must be >= 0")
        return v

    @field_validator(
        "v2_validation_stale_symbol_threshold",
        "v2_validation_stale_symbol_cooldown_seconds",
        "v2_validation_rest_symbol_threshold",
        "v2_validation_rest_symbol_cooldown_seconds",
    )
    @classmethod
    def validate_stale_symbol_settings(cls, v):
        if v < 0:
            raise ValueError("v2_validation stale symbol settings must be >= 0")
        return v

    @field_validator("exchanges", mode="before")
    @classmethod
    def parse_exchanges(cls, v):
        if isinstance(v, list):
            cls._exchanges_source_tracker = "list"
            return v
        if isinstance(v, str):
            stripped = v.strip()
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        cls._exchanges_source_tracker = "json"
                        return parsed
                except json.JSONDecodeError:
                    pass
            parts = [
                item.strip().lower() for item in stripped.split(",") if item.strip()
            ]
            if parts:
                logging.getLogger(__name__).warning(
                    "EXCHANGES parsed from CSV; prefer JSON array"
                )
                cls._exchanges_source_tracker = "csv"
                return parts
            return []
        return v

    @field_validator("ws_exchanges", mode="before")
    @classmethod
    def parse_ws_exchanges(cls, v):
        if v is None:
            return None
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return None
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError("WS_EXCHANGES must be valid JSON list") from e
                if not isinstance(parsed, list):
                    raise ValueError("WS_EXCHANGES must be a JSON list")
                return parsed
            return [
                item.strip().lower() for item in stripped.split(",") if item.strip()
            ]
        return v

    @field_validator("core_exchanges", mode="before")
    @classmethod
    def parse_core_exchanges(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError("CORE_EXCHANGES must be valid JSON list") from e
                if not isinstance(parsed, list):
                    raise ValueError("CORE_EXCHANGES must be a JSON list")
                return parsed
            return [
                item.strip().lower() for item in stripped.split(",") if item.strip()
            ]
        return v

    @field_validator("periphery_exchanges", mode="before")
    @classmethod
    def parse_periphery_exchanges(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        "PERIPHERY_EXCHANGES must be valid JSON list"
                    ) from e
                if not isinstance(parsed, list):
                    raise ValueError("PERIPHERY_EXCHANGES must be a JSON list")
                return parsed
            return [
                item.strip().lower() for item in stripped.split(",") if item.strip()
            ]
        return v

    @model_validator(mode="after")
    def assign_exchanges_source(self):
        self.exchanges_source = self._exchanges_source_tracker
        return self

    @field_validator("withdrawal_fee_cache_lifetime")
    @classmethod
    def validate_cache_lifetime(cls, v):
        """Validate withdrawal_fee_cache_lifetime is within acceptable range."""
        if v < 60:
            raise ValueError(
                "withdrawal_fee_cache_lifetime must be at least 60 seconds (1 minute)"
            )
        if v > 86400:
            raise ValueError(
                "withdrawal_fee_cache_lifetime must not exceed 86400 seconds (24 hours)"
            )
        return v

    @field_validator("exchange_symbol_limits_json")
    @classmethod
    def validate_exchange_symbol_limits_json(cls, v):
        if not v:
            return v
        try:
            parsed = json.loads(v)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Invalid EXCHANGE_SYMBOL_LIMITS_JSON: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("EXCHANGE_SYMBOL_LIMITS_JSON must be a JSON object")
        for ex_id, value in parsed.items():
            if not isinstance(ex_id, str):
                raise ValueError("EXCHANGE_SYMBOL_LIMITS_JSON keys must be strings")
            try:
                limit_int = int(value)
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] must be int"
                ) from e
            if limit_int < 0:
                raise ValueError(f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] must be >= 0")
            if limit_int > 5000:
                raise ValueError(
                    f"EXCHANGE_SYMBOL_LIMITS_JSON[{ex_id!r}] too large (>5000)"
                )
        return v

    @field_validator("withdrawal_fee_fetch_timeout")
    @classmethod
    def validate_fetch_timeout(cls, v):
        """Validate withdrawal_fee_fetch_timeout is within acceptable range."""
        if v < 5:
            raise ValueError("withdrawal_fee_fetch_timeout must be at least 5 seconds")
        if v > 60:
            raise ValueError("withdrawal_fee_fetch_timeout must not exceed 60 seconds")
        return v

    @field_validator("log_level_console", "log_level_file")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid Python logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper

    @field_validator("log_max_bytes")
    @classmethod
    def validate_log_max_bytes(cls, v: int) -> int:
        """Validate log file size is within reasonable bounds (1 MB - 1 GB)."""
        min_size = 1048576  # 1 MB
        max_size = 1073741824  # 1 GB
        if not min_size <= v <= max_size:
            raise ValueError(
                f"log_max_bytes must be between {min_size} (1 MB) and {max_size} (1 GB)"
            )
        return v

    @field_validator("log_backup_count")
    @classmethod
    def validate_log_backup_count(cls, v: int) -> int:
        """Validate backup count is reasonable (0-20)."""
        if not 0 <= v <= 20:
            raise ValueError("log_backup_count must be between 0 and 20")
        return v

    @field_validator("log_sample_ratio")
    @classmethod
    def validate_log_sample_ratio(cls, v: int) -> int:
        """Validate sampling ratio is positive (1-1000)."""
        if not 1 <= v <= 1000:
            raise ValueError("log_sample_ratio must be between 1 and 1000")
        return v

    @field_validator("log_sample_interval_seconds")
    @classmethod
    def validate_log_sample_interval(cls, v: float) -> float:
        """Validate sampling interval is non-negative and reasonable (0-60)."""
        if not 0.0 <= v <= 60.0:
            raise ValueError("log_sample_interval_seconds must be between 0.0 and 60.0")
        return v

    @field_validator("alert_verify_delay_seconds")
    @classmethod
    def validate_alert_verify_delay_seconds(cls, v: float) -> float:
        if v < 0:
            raise ValueError("alert_verify_delay_seconds must be >= 0")
        if v > 10:
            raise ValueError("alert_verify_delay_seconds must be <= 10")
        return v

    @field_validator(
        "alert_verify_fee_tolerance_pct",
        "alert_verify_fee_tolerance_base",
        "fee_live_validation_tolerance_pct",
        "fee_live_validation_tolerance_base",
    )
    @classmethod
    def validate_fee_tolerances(cls, v: float, info):  # type: ignore[override]
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0")
        return v

    @field_validator("circuit_failure_threshold")
    @classmethod
    def validate_circuit_failure_threshold(cls, v: int) -> int:
        """Validate failure threshold is reasonable (1-50)."""
        if not 1 <= v <= 50:
            raise ValueError("circuit_failure_threshold must be between 1 and 50")
        return v

    @field_validator("circuit_recovery_timeout_seconds")
    @classmethod
    def validate_circuit_recovery_timeout(cls, v: int) -> int:
        """Validate recovery timeout (30s - 30min)."""
        if not 30 <= v <= 1800:
            raise ValueError(
                "circuit_recovery_timeout_seconds must be between 30 and 1800"
            )
        return v

    @field_validator("circuit_half_open_max_calls")
    @classmethod
    def validate_circuit_half_open_max_calls(cls, v: int) -> int:
        """Validate half-open max calls (1-10)."""
        if not 1 <= v <= 10:
            raise ValueError("circuit_half_open_max_calls must be between 1 and 10")
        return v

    def get_suppress_prefixes(self) -> list[str]:
        """Parse comma-separated suppress prefixes into a list."""
        if not self.log_suppress_prefixes:
            return []
        return [p.strip() for p in self.log_suppress_prefixes.split(",") if p.strip()]

    def get_access_control_ids(self) -> set[str]:
        """Parse ACCESS_CONTROL_IDS from JSON array or comma-separated string.

        Supports formats:
        - JSON array: '["123456789","987654321"]'
        - Comma-separated: '123456789,987654321'
        - Single value: '123456789'

        Returns:
            Set of authorized chat IDs, or empty set if not configured.
        """
        if not self.access_control_ids:
            return set()

        stripped = self.access_control_ids.strip()
        if not stripped:
            return set()

        # Try JSON array first
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return {str(item).strip() for item in parsed if item}
            except json.JSONDecodeError:
                pass

        # Fallback to comma-separated
        return {item.strip() for item in stripped.split(",") if item.strip()}

    @model_validator(mode="after")
    def validate_trading_phase(self) -> "Settings":
        """
        CRITICAL SECURITY: Prevent real trading during testing phases.

        Phases R1-R3 are testing/dry-run only.
        Real trading (TRADING_ENABLED=true) only allowed from Phase R5+.
        """
        if self.trading_enabled and self.current_phase in ["R1", "R2", "R3", "R4"]:
            raise ValueError(
                f"❌ TRADING_ENABLED cannot be true during phase {self.current_phase}. "
                f"Real trading is only allowed from Phase R5+. "
                f"Current phase is for testing/dry-run only."
            )
        return self

    @model_validator(mode="after")
    def validate_daily_fee_report(self) -> "Settings":
        """Fail fast if daily fee report enabled without tech chat_id."""
        if self.enable_daily_fee_report and not self.telegram_tech_chat_id:
            raise ValueError(
                "❌ ENABLE_DAILY_FEE_REPORT=true requires TELEGRAM_TECH_CHAT_ID. "
                "Set TELEGRAM_TECH_CHAT_ID or disable the feature."
            )
        return self


settings = Settings()
