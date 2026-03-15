# Backtest API

Date: 2026-03-15

## Summary

This slice added a small historical-trade backtest API so research no longer depends on ad hoc notebook CSV workflows.

## Main Changes

- added `mykalshi/research/backtest.py`
- added `TradeSignal` for strategy outputs
- added `TradeBacktester` for:
  - direct runs on supplied trade lists
  - runs sourced from `mykalshi.historical`
- added structured result types:
  - `BacktestFill`
  - `BacktestMark`
  - `BacktestResult`
- updated README with research-layer examples

## Verification

- unit tests passed for:
  - yes-side round-trip execution
  - historical-module integration
- live historical dry run passed with:
  - real historical trade fetch
  - no-op strategy
  - structured summary output

## Follow-Up

- dataset replay helpers
- broader websocket channel support
- optional fee models and example strategies
