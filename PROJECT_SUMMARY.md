# Project Summary (Portfolio)

## What This Is

`Parsertang` is a real-time Python system for monitoring cross-exchange price opportunities and filtering out false positives before alerting.

It combines:

- WebSocket-first market data ingestion
- REST validation for executable prices/liquidity
- fee-aware net-profit calculation
- safety gates (`TRUTH`, validation health)
- operator-grade diagnostics (why a signal was rejected)

## Why It Is a Strong Engineering Sample

This project demonstrates practical backend engineering under real-world uncertainty:

- async IO orchestration (`WS + REST`)
- resilience against flaky third-party APIs and reconnect behavior
- observability-driven debugging (health snapshots, funnel reasons, recovery events)
- safety-first decisioning (fail-closed alerts)
- regression testing around exchange-specific edge cases

## Example Engineering Problem Solved

When alerts disappeared, the investigation showed it was **not just market conditions**. The root issue included degraded exchange coverage (one exchange stream collapsing to effectively zero active symbols), which narrowed the opportunity surface and changed the validation funnel.

This is the kind of production debugging problem I like to work on: prove the bottleneck with logs/metrics first, then fix the right layer.

## Stack

- Python 3.11
- asyncio
- ccxt / ccxt.pro
- Pydantic
- python-telegram-bot
- pytest

## Good Entry Points

- `README.md`
- `docs/architecture.md`
- `docs/incidents/no-alerts-root-cause.md`
- `tests/test_streams_preload_markets.py`

