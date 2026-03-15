# Trade Auto-Routing

Date: 2026-03-15

## Summary

This slice added a first live/historical auto-routing helper so research code can fetch Kalshi trades without manually splitting sources.

## Main Changes

- added `mykalshi/routing.py`
- added:
  - `get_cutoff_timestamps()`
  - `resolve_trade_source(...)`
  - `get_trades_auto(...)`
  - `get_trades_dataframe_auto(...)`
- exported `routing` from the top-level package
- documented trade auto-routing in `README.md` and `USAGE.md`

## Verification

- unit tests passed for:
  - active-market live routing
  - fallback to historical on live 404
  - finalized-market historical routing
  - merged historical/live range handling
- live dry runs passed for:
  - archived ticker routing to `historical`
  - live ticker routing to `live`

## Follow-Up

- auto-routing helpers beyond trades
- more execution models for backtests
