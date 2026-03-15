# Backtest Correctness Pass

Date: 2026-03-15

## Summary

This slice hardened the new event-driven Kalshi backtest engine around reservations, deterministic order lifecycle behavior, settlement handling, and legacy-wrapper compatibility.

## Main Changes

- added cash and inventory reservation tracking in `mykalshi/research/engine/portfolio.py`
- expanded simulated order state in `mykalshi/research/engine/orders.py`
- centralized settlement handling, explicit cancel/reject transitions, and per-event deterministic request/fill processing in `mykalshi/research/engine/core.py`
- extended event/reporting models in:
  - `mykalshi/research/engine/events.py`
  - `mykalshi/research/engine/execution.py`
  - `mykalshi/research/engine/reporting.py`
- migrated `mykalshi/research/backtest.py` onto `EventDrivenBacktestEngine` while preserving the legacy `TradeBacktester` API
- added compatibility adapters so old strategy code still works with:
  - `TradeSignal`
  - `PositionTargetSignal`
  - `ImmediateTradeExecutionModel`
  - legacy fee models
- expanded engine correctness coverage in `tests/test_research_event_engine.py`

## Verification

- targeted wrapper and engine tests passed
- full suite passed: `python -m unittest discover -s tests -v`
- compile check passed: `python -m compileall mykalshi recorder_script.py`
- live read-only dry run passed through `TradeBacktester.run_on_historical_trades(...)`

## Follow-Up

- improve execution realism beyond the immediate compatibility fill model
- add queue-style and orderbook-aware fill assumptions
- expand settlement sourcing and richer performance analytics
