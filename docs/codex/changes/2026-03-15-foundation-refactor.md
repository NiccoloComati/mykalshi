# Foundation Refactor

Date: 2026-03-15

## Summary

This slice reorganized the repo from a loose set of endpoint wrappers into a reusable package foundation.

## Main Changes

- added `KalshiConfig` for environment-aware credential loading
- added `KalshiAuthSigner` for request signing
- added `KalshiClient` for reusable HTTP transport and centralized error handling
- moved wrapper modules onto the shared client/transport layer
- added `historical.py` for archived endpoints
- extracted reusable recorder logic into `mykalshi.recorder`
- added packaging metadata, README, and basic tests
- fixed wrapper issues including authenticated orderbook access and batch cancel request payloads

## Verification

- unit tests passed in local `.venv`
- package import passed
- compile check passed
- read-only live REST checks passed with production-configured credentials
- read-only live websocket subscription handshake passed

## Follow-Up

- websocket snapshot/delta capture module
- storage sinks for SQLite and Parquet
- simple backtest API on stored and historical data
