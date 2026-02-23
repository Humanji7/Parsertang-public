# Incident Case: "No Alerts" Was Not a Market Problem

## Problem Statement

The system appeared healthy (service running, live logs, active streams), but no alerts were being sent for an extended period.

A common wrong conclusion would be: "There are simply no opportunities in the market."

## Investigation Approach

I traced the alert pipeline end-to-end instead of tuning thresholds blindly:

1. Confirm service health and WS activity
2. Confirm whether alerts had ever been sent recently
3. Inspect `TRUTH` behavior (blocked vs passing)
4. Inspect fee-validation outcomes (`ok=True` vs `ok=False`)
5. Inspect exchange coverage / symbol counts by exchange (`WS HEALTH`)
6. Compare pre/post behavior over multiple days

## What the Data Showed

### 1) Alerts did exist, but only in a narrow time window

- Confirmed historical `ALERT SENT` entries existed
- Real delivered alerts were concentrated on a short period (2026-02-19 to 2026-02-20)

### 2) The pipeline after that was alive, but no candidates passed fee-validation

- `TRUTH OK` events continued (system was not "dead")
- `FEE VALIDATION ok=True` dropped to zero in the investigated period
- `FEE VALIDATION ok=False reason=rest_net_below_threshold` dominated

This means the system was still evaluating candidates, but the net after costs was not good enough.

### 3) More importantly: coverage had degraded, so we were evaluating a much narrower market slice

The key issue was not only thresholds/fees. Exchange coverage degraded:

- `gate` exchange showed `alloc=50`, but actual live symbols collapsed to `0/0sym` (and later effectively `1sym` in many periods)
- This sharply reduced the search surface
- The effective opportunity flow became concentrated mostly in `bybit <-> okx`

### 4) Surviving profitable cases were over-concentrated

When looking at fee-passing snapshots during the relevant period, they were concentrated in a single symbol/pair route (example: `INJ/USDT`, `bybit -> okx`), instead of broad market coverage.

## Root Cause (Practical)

The "no alerts" symptom was caused by a combination of:

- degraded exchange coverage (especially `gate` stream collapse)
- strict but valid fee-aware profitability filtering
- liquidity validation rejections on REST snapshots

In short:

> It was not just "market is dead".  
> We were looking at a narrowed and degraded slice of the market.

## Why This Matters (Engineering Lesson)

Without observability, the team could easily have:

- lowered thresholds incorrectly
- blamed commissions
- blamed "no market opportunities"
- broken the safety model to force alerts

Instead, the correct response is to restore coverage and validate the funnel at each stage.

## Takeaway

This incident is a good example of production debugging in an external-API-heavy system:

- verify assumptions with logs/metrics
- measure the funnel
- separate "market conditions" from "system coverage degradation"
- avoid changing risk controls before proving the real bottleneck

