# Replay Lifecycle And Reporting

Date: 2026-03-15

## Summary

This slice makes replay backtests more complete and more usable by enriching incomplete replay timelines with expiration/settlement events from Kalshi metadata and by adding real result analytics on top of the event-driven engine.

## Main Changes

- added `mykalshi/research/lifecycle.py` for replay lifecycle enrichment and metadata resolution
- added `ReplayBacktester.enrich_replay_event_stream(...)`
- made `ReplayBacktester.run_on_replay_event_stream(...)` and `run_on_captured_dataset(...)` enrich settlement and expiration events by default
- taught `mykalshi/research/engine/replay.py` to map replay settlement dicts into engine `SettlementEvent`s
- expanded `BacktestRunResult` with:
  - position snapshots
  - per-market summaries
  - turnover, exposure, and return metrics
  - raw record exports
  - lazy pandas dataframe exports
- exported `PositionSnapshot` and `MarketPerformanceSummary` through `mykalshi.research`
- added focused lifecycle coverage in `tests/test_research_replay_backtest.py`
- added focused reporting/export coverage in `tests/test_research_reporting.py`

## Verification

- `python -m unittest tests.test_research_reporting tests.test_research_replay_backtest tests.test_research_event_engine -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- local smoke runs passed for:
  - `ReplayBacktester.enrich_replay_event_stream(...)`
  - `BacktestRunResult.market_summaries()`
  - `BacktestRunResult.position(...)`
  - `BacktestRunResult.to_dataframes()`

## Follow-Up

- improve higher-level research ergonomics around discovery, load, replay, and backtest workflows
- add stronger market-family enrichment when replay spans related contracts
- continue the roadmap into safer trading workflows on top of the improved research core
