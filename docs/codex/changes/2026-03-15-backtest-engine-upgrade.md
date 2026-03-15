# Backtest Engine Upgrade

Date: 2026-03-15

## Summary

This slice moved the backtest layer closer to a real engine instead of a thin trade-loop helper.

## Main Changes

- upgraded `mykalshi/research/backtest.py` with:
  - explicit order records
  - explicit filled vs rejected execution outcomes
  - limit-price support
  - execution model abstraction
  - fee-model abstraction
  - Kalshi-style taker fee model
  - richer summary metrics including fees, PnL, and drawdown
- exported the new fee and execution helpers from `mykalshi.research`
- updated README and usage docs for the richer backtest API

## Verification

- unit tests passed for:
  - round-trip trade execution
  - historical-module integration
  - rejected limit orders
  - Kalshi-style taker fee application
- live historical dry run passed with:
  - real archived trade data
  - limit-price strategy
  - Kalshi-style fee model

## Follow-Up

- broader websocket channel capture
- historical/live auto-routing
- more execution models
