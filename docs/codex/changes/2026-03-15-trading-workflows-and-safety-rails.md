# 2026-03-15 Trading Workflows And Safety Rails

## Summary

Added a higher-level live trading layer on top of the existing raw wrappers.

## What Changed

- added `mykalshi/trading_workflows.py`
  - normalized account state snapshots for balance, limits, orders, positions, and resting exposure
  - market-context snapshot helper combining account state, market metadata, and live order book
  - higher-level order helpers for submit, amend, replace, stale-order cancellation, and position flattening
  - `TradingSafetyPolicy` with:
    - production write blocking by default
    - dry-run mode
    - max order quantity
    - max order risk
    - max open orders per market
    - max total resting exposure
    - allowed/blocked ticker filters
    - JSONL audit logging
- updated `mykalshi/trading.py`
  - `get_balance(...)` now accepts `subaccount`
  - `amend_order(...)` now supports the current Kalshi amend-order body shape while preserving the old `price` alias
- exported the new module from `mykalshi/__init__.py`
- added coverage in `tests/test_trading_workflows.py`
- updated `README.md` and `USAGE.md`

## Verification

- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- live read-only and dry-run checks:
  - `TradingSession.snapshot(order_status="resting")`
  - `TradingSession.market_snapshot(...)`
  - `TradingSession.buy_yes(..., dry_run=True)`
  - production write blocking via `TradingSession.buy_yes(...)`
  - `TradingSession.cancel_stale_orders(..., dry_run=True)`

## Notes

- the root `.env` still resolves to production, so `TradingSession` blocks writes by default unless `allow_production_writes=True` or `dry_run=True`
- this pass adds workflow ergonomics and safety rails, not advanced live execution logic like queue-aware order management
