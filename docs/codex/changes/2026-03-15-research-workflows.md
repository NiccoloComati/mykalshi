# Research Workflows

Date: 2026-03-15

## Summary

This slice adds a higher-level research workflow layer so common discovery, dataset loading, and replay/historical backtests no longer require manual glue code across `discovery`, `datasets`, and `backtest`.

## Main Changes

- added `mykalshi/research/workflows.py`
- added `DiscoveredMarket` as a typed wrapper around discovery results
- added `ReplayDataset` as a higher-level wrapper around market-data/orderbook sources and merged replay timelines
- added `ResearchSession` with:
  - `search_markets(...)`
  - `resolve_market(...)`
  - `load_replay_dataset(...)`
  - `run_replay_backtest(...)`
  - `run_historical_backtest(...)`
- exported the new workflow surface through `mykalshi.research`
- added focused workflow coverage in `tests/test_research_workflows.py`

## Verification

- `python -m unittest tests.test_research_workflows tests.test_research_replay_backtest tests.test_research_reporting -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- local smoke runs passed for:
  - `ResearchSession.load_replay_dataset(...)`
  - `ReplayDataset.backtest(...)`
  - `ResearchSession.run_replay_backtest(...)`

## Follow-Up

- improve higher-level research ergonomics further with repeatable end-to-end workflows and examples
- upgrade discovery ergonomics around category/subdomain semantics and safer narrowing of market universes
- continue the roadmap into live trading workflows and safety rails
