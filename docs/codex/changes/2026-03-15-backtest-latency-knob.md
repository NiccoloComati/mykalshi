# Backtest Latency Knob Pass

Date: 2026-03-15

## Summary

This slice adds an optional per-order event-latency knob to the event-driven backtest engine so strategies can model delayed order entry/fill eligibility without breaking existing behavior.

## Main Changes

- added `latency_events` to `OrderRequest` and strategy order submission helpers
- extended simulated order state with:
  - `latency_events`
  - `remaining_latency_events`
- engine fill processing now decrements latency counters per market event and only evaluates fills once latency is exhausted
- added request validation for non-negative latency
- added focused tests for:
  - delayed aggressive fill eligibility
  - rejection of negative latency values

## Verification

- `python -m unittest tests.test_research_event_engine -v`
- `python -m unittest discover -s tests -v`
