# Backtest Execution Realism Pass

Date: 2026-03-15

## Summary

This slice strengthened the event-driven backtest engine's execution realism while preserving legacy wrappers and the broader research API surface.

## Main Changes

- expanded `mykalshi/research/engine/execution.py` with explicit pluggable fill models:
  - `ImmediateCompatibilityFillModel` for simple immediate behavior
  - `OrderbookAwareFillModel` for aggressive/passive distinctions and passive-queue behavior
  - `KalshiBinaryFillModel` now routes through the orderbook-aware implementation as the default
- extended simulated order/fill metadata to carry execution intent and queue state:
  - resting price
  - queue ahead quantity
  - liquidity role on fills
- preserved legacy `TradeBacktester` compatibility by keeping the legacy adapter path and exposing aggressive role metadata in that shim
- exported new fill models through `mykalshi.research` and `mykalshi.research.engine`
- added focused engine tests for:
  - passive orders resting without immediate fill
  - passive partial fills across replay events
  - aggressive vs passive execution differences
  - deterministic queue-ahead delay behavior

## Verification

- targeted event-engine and backtest tests passed:
  - `python -m unittest tests.test_research_event_engine tests.test_research_backtest -v`

## Follow-Up

- improve queue depletion signals using richer orderbook delta semantics (not just prints)
- add explicit maker/taker fee plumbing tied to `liquidity_role`
- extend replay adapters to capture additional microstructure hints when available
