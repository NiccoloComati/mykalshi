# First-Class Replay Backtester

Date: 2026-03-15

## Summary

This slice adds a higher-level backtest workflow over merged captured datasets so replay-based research no longer requires manual `MarketDataReplay` and `EventDrivenBacktestEngine` wiring.

## Main Changes

- added `ReplayBacktester` in `mykalshi/research/backtest.py`
- added `ReplayBacktester.run_on_replay_event_stream(...)`
- added `ReplayBacktester.run_on_captured_dataset(...)`
- kept the legacy `TradeBacktester` surface unchanged for historical trade workflows
- exported `ReplayBacktester` through `mykalshi.research`
- added focused replay-wrapper coverage in `tests/test_research_replay_backtest.py`
- updated usage/docs and repo context to treat replay backtesting as a first-class workflow

## Verification

- `python -m unittest tests.test_research_replay_backtest tests.test_research_backtest tests.test_research_datasets -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- local smoke runs passed for:
  - `ReplayBacktester.run_on_replay_event_stream(...)`
  - `ReplayBacktester.run_on_captured_dataset(...)`

## Follow-Up

- enrich settlement and expiration sourcing for replay datasets
- add stronger analytics and result-export helpers
- improve higher-level research ergonomics around discovery, load, replay, and backtest
