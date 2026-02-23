# Architecture Overview

## Goal

Detect cross-exchange opportunities while reducing false positives caused by stale data, broken streams, low liquidity, or incorrect fee assumptions.

## Core Flow

1. WebSocket streams provide BBO/order book updates (`WS-first`)
2. In-memory state tracks fresh snapshots per exchange/symbol
3. Opportunity evaluation builds candidate pairs from overlapping symbols
4. REST validation checks executable prices/liquidity
5. Fee calculation computes net profit (trade + withdrawal/network fees)
6. Safety gates (TRUTH / health validation) suppress unreliable alerts
7. Telegram alert is sent, or exact rejection reason is logged

## Key Design Choices

- `WS-first` for latency and symbol coverage
- `REST second-pass validation` to avoid sending stale/phantom edges
- `fail-closed` gating (better no alert than false alert)
- `reasoned rejection logging` (why a candidate was blocked)
- `observability built into business flow` (not only infra logs)

## Main Components (High-Level)

- `src/parsertang/streams.py`
  - WS subscriptions, reconnect/retry behavior, preload/market handling
- `src/parsertang/core/orchestrator.py`
  - runtime coordination, symbol allocation, health/recovery orchestration
- `src/parsertang/core/opportunity_evaluator.py`
  - opportunity checks, validation funnel, fee-aware net calculations, alert decisions
- `src/parsertang/withdrawal_fees.py`
  - dynamic fee lookup and normalization paths
- `src/parsertang/network_aliases.py` / `src/parsertang/networks.py`
  - network alias normalization and network selection logic
- `src/parsertang/alerts.py`
  - Telegram delivery and technical alerts
- `src/parsertang/truth_gate.py` / `src/parsertang/v2/*`
  - validation/truth accounting and health/safety gating

## Reliability / Ops Thinking Demonstrated

- WebSocket health metrics per exchange (`symbols`, `alloc`, `stale`)
- Recovery events (`first_update`, reconnect behavior)
- Validation funnel counters (where opportunities die and why)
- Fee validation reason breakdown (`rest_net_below_threshold`, etc.)
- Test coverage for exchange-specific regressions

## Why This Repo Is a Useful Engineering Sample

It demonstrates applied backend engineering in a noisy real-world domain:

- async IO coordination (`WS + REST`)
- defensive programming against flaky external systems
- production debugging via logs/metrics instead of guesswork
- domain logic + reliability logic working together

