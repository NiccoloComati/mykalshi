# Market Data Websocket Capture

Date: 2026-03-15

## Summary

This slice broadened the research websocket layer beyond orderbook-only capture.

## Main Changes

- added normalized `ticker` and `trade` event builders
- added generic market-data message normalization
- added multi-channel capture via `capture_market_data(...)`
- added generic market-data sinks for:
  - SQLite
  - Parquet
- added generic market-data loading helpers

## Verification

- unit tests passed for:
  - ticker normalization
  - trade normalization
  - SQLite round-trip
  - Parquet round-trip
- live dry runs passed for:
  - ticker websocket snapshot capture
  - trade websocket capture
  - SQLite and Parquet storage round-trip

## Follow-Up

- historical/live auto-routing helpers
- more websocket channels
