# Storage Sinks

Date: 2026-03-15

## Summary

This slice added durable local storage for normalized orderbook websocket events.

## Main Changes

- added `mykalshi/research/storage.py`
- added `SQLiteOrderbookSink` for local relational storage
- added `ParquetOrderbookSink` for append-only parquet datasets
- added `MultiOrderbookSink` to fan out one capture stream into multiple sinks
- updated optional storage dependencies to include `pyarrow`

## Verification

- unit tests passed for SQLite and Parquet sink round-trips
- live capture passed with one websocket event written to:
  - SQLite
  - Parquet
- SQLite and Parquet read paths both returned the stored event

## Follow-Up

- backtest API on historical trades
- higher-level dataset replay helpers
