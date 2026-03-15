# Event-Driven Backtest Engine

Date: 2026-03-15

## Summary

This slice introduced a new modular event-driven backtest engine without breaking the existing wrapper-based research workflow.

## Main Changes

- added `mykalshi/research/engine/` with separate modules for:
  - replay
  - events and market state
  - strategy context
  - order management
  - execution simulation
  - portfolio/accounting
  - reporting
  - engine core
- added `EventDrivenBacktestEngine`
- added `HistoricalTradeReplay` and `MarketDataReplay`
- added explicit order, cancel, fill, mark, and settlement event models
- exported the new engine surface from `mykalshi.research`
- added an in-repo design note at `docs/backtest-engine-architecture.md`

## Verification

- focused engine tests passed for:
  - trade replay
  - partial fills
  - resting-order cancellation
- compile check passed
- live dry runs passed for:
  - archived trade replay through the new engine
  - live ticker replay through the new engine

## Follow-Up

- migrate `TradeBacktester` compatibility paths onto the new engine
- add order reservation and richer cancel/replace logic
- improve queue and orderbook execution realism
