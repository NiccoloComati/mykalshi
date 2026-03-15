# Websocket Orderbook Capture

Date: 2026-03-15

## Summary

This slice added the first live research-layer component: authenticated websocket orderbook capture with normalized snapshot and delta records.

## Main Changes

- added `mykalshi/fixed_point.py` for shared fixed-point conversions
- added `mykalshi/orderbook.py` for order book parsing and in-memory state
- added `mykalshi/research/websocket.py` with:
  - `SubscriptionRequest`
  - `KalshiWebsocketClient`
  - normalized orderbook event output
- updated `mykalshi/recorder.py` to handle the current REST `orderbook_fp` shape instead of only the legacy `orderbook` shape

## Verification

- unit tests passed, including orderbook normalization tests
- live capture passed with:
  - authenticated websocket subscribe
  - `orderbook_snapshot` receipt
  - normalized event output through the new client

## Follow-Up

- SQLite sink
- Parquet sink
- simple backtest API on historical trades
