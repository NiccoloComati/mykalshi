# Discovery Layer

Date: 2026-03-15

## Summary

This slice added a higher-level discovery/query layer so users can target Kalshi series, events, and markets without manually stitching endpoint wrappers together.

## Main Changes

- added `mykalshi/discovery.py`
- added:
  - `search_series(...)`
  - `search_events(...)`
  - `search_markets(...)`
  - `resolve_market(...)`
- exported `discovery` from the top-level package
- documented targeted discovery in `README.md` and `USAGE.md`

## Verification

- unit tests passed for:
  - case-insensitive series filtering
  - series-scoped event filtering
  - market search enrichment
  - exact-event direct lookup path
- live dry runs passed for:
  - series search
  - series-scoped market search
  - exact market resolution for `KXELONMARS-99`

## Follow-Up

- dataset loading and replay helpers
- broader websocket channel coverage
