# Merged Replay Streams for Research Backtests

Date: 2026-03-15

## Summary

This slice makes replayed market-data backtests more first-class by adding reusable dataset helpers that load and merge stored trade/ticker events with reconstructed orderbook events into one deterministic timeline.

## Main Changes

- added `merge_replay_event_streams(...)` in `mykalshi/research/datasets.py`
  - merges market-data events with orderbook events
  - replays orderbook snapshots/deltas through `replay_orderbook_events(...)` before merge
  - returns one timestamp-ordered stream ready for `MarketDataReplay.from_market_data_events(...)`
- added `load_replay_event_stream(...)` in `mykalshi/research/datasets.py`
  - loads market-data and orderbook sources via existing sink/path loaders
  - applies optional ticker filter
  - merges into a single replay timeline
- exported both helpers through `mykalshi.research`
- expanded dataset tests to verify ordering and combined-source behavior

## Verification

- `python -m unittest tests.test_research_datasets tests.test_research_event_engine -v`
- `python -m unittest discover -s tests -v`
