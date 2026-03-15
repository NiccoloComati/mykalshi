# Dataset Replay

Date: 2026-03-15

## Summary

This slice made captured orderbook datasets reusable by adding load and replay helpers on top of the SQLite and Parquet sinks.

## Main Changes

- added `mykalshi/research/datasets.py`
- added:
  - `load_orderbook_events(...)`
  - `replay_orderbook_events(...)`
  - `orderbook_events_to_dataframe(...)`
- aligned Parquet sink loading with SQLite sink filtering arguments
- documented stored-dataset replay in `README.md` and `USAGE.md`

## Verification

- unit tests passed for:
  - SQLite dataset loading
  - Parquet dataset loading
  - snapshot/delta replay
- live dry run passed for:
  - websocket capture to SQLite
  - reload from SQLite
  - replay through reconstructed orderbook state

## Follow-Up

- broader websocket channel coverage
- strategy and fee-model helpers
