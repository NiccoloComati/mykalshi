# Strategy Layer

Date: 2026-03-15

## Summary

This slice moved the backtest interface from raw order actions toward strategy-driven target positions and reusable signal adapters.

## Main Changes

- upgraded `mykalshi/research/backtest.py` with:
  - `PositionTargetSignal`
  - staged position transitions with `max_trade_quantity`
  - sequential side-flip handling such as `sell_no` before `buy_yes`
  - rejected order recording for risk failures like insufficient cash instead of aborting the run
  - richer summary counts for filled vs rejected orders
- added `mykalshi/research/strategies.py` with:
  - `target_yes(...)`
  - `target_no(...)`
  - `target_flat(...)`
  - `ThresholdSignalStrategy`
  - `ProbabilityEdgeStrategy`
- exported the new strategy helpers from `mykalshi.research`
- updated README, usage docs, and current Codex context

## Verification

- unit tests passed for:
  - target-position flips
  - staged transitions with trade budgets
  - insufficient-cash rejection handling
  - threshold strategy integration
  - probability-edge hysteresis behavior
- compile check passed
- live archived-data dry runs passed for:
  - `ProbabilityEdgeStrategy`
  - `ThresholdSignalStrategy`
  - target-position helper factories

## Follow-Up

- event-driven replay on stored market-data datasets for backtests
- more websocket channels
- broader auto-routing
