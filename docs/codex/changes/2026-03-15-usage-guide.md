# Usage Guide

Date: 2026-03-15

## Summary

This slice documented how to actually use the current repo and fixed one small robustness issue in websocket capture shutdown.

## Main Changes

- added `USAGE.md` with runnable workflows for:
  - live market discovery
  - live market data
  - archived historical data
  - websocket orderbook capture
  - SQLite and Parquet sinks
  - historical backtests
- updated `README.md` to point to `USAGE.md`
- updated websocket capture to flush sinks in a `finally` block

## Why

The previous examples used placeholder strings such as `"YOUR_TICKER"`, which were too easy to copy literally and caused misleading failures.

## Verification

- unit tests passed after the websocket cleanup change
- usage guidance now documents the exact failure modes seen in manual terminal testing
