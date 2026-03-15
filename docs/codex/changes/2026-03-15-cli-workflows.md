# 2026-03-15 CLI Workflows

## Summary

Added a real `mykalshi` CLI over the existing discovery, capture, replay, backtest, and trading workflow layers.

## What Changed

- added `mykalshi/cli.py`
  - `discover` commands for series, events, markets, and grouped market universes
  - `capture` commands for orderbook and generic market-data websocket collection
  - `replay summary` for stored SQLite/parquet replay datasets
  - `backtest historical` and `backtest replay` with strategy loading via import path
  - `trading` commands for snapshots, market context, dry-run order planning, order replacement, flattening, and stale-order cancellation
- added `mykalshi/__main__.py` so `python -m mykalshi ...` works
- updated `pyproject.toml` with the `mykalshi` console script entry point
- added CLI coverage in `tests/test_cli.py` and fixture strategies in `tests/cli_fixtures.py`
- updated `README.md`, `USAGE.md`, and `docs/codex/current-context.md`

## Verification

- `pip install -e .[analysis,storage,websocket]`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- live CLI smoke checks passed for:
  - `python -m mykalshi discover markets --status open --limit 1`
  - `python -m mykalshi trading snapshot`
  - `python -m mykalshi capture market-data --channels ticker --market-ticker ... --send-initial-snapshot --max-events 1`
  - `python -m mykalshi trading plan-order ...` in dry-run mode
  - `python -m mykalshi backtest historical ... --strategy tests.cli_fixtures:historical_strategy`
  - `mykalshi --help`

## Notes

- CLI trading mutation commands default to dry-run planning and require `--execute` for live writes
- the CLI strategy loader currently expects Python import paths and does not yet provide a strategy registry or notebook-style inline strategy definition flow
