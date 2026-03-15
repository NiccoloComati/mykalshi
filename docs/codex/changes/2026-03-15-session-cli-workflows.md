# 2026-03-15 Session CLI Workflows

## Summary

Added standardized replay-session workflows so websocket capture, replay inspection, and replay backtests can share one directory-based format instead of manually passing separate SQLite and parquet paths around.

## Implementation

- added `SplitMarketCaptureSink` in `mykalshi/research/storage.py` to route mixed websocket capture into market-data and orderbook sinks in one pass
- added `CaptureSession` in `mykalshi/research/workflows.py`
- added `ResearchSession.open_capture_session(...)`
- added `ResearchSession.capture_market_session(...)`
- extended `ResearchSession.load_replay_dataset(...)` and `run_replay_backtest(...)` with `session_dir=...`
- added discovery-based market resolution inside `capture_market_session(...)` when `market_ticker` is not provided
- normalized response-style market status values like `active` to the filter values Kalshi currently accepts on `GET /markets`
- extended the CLI with:
  - `mykalshi capture session ...`
  - `mykalshi replay summary --session-dir ...`
  - `mykalshi backtest replay --session-dir ...`

## Verification

- `.\.venv\Scripts\python -m unittest discover -s tests -v`
- `.\.venv\Scripts\python -m compileall mykalshi recorder_script.py`
- live CLI smoke checks passed for:
  - `python -m mykalshi capture session <temp-dir> --market-ticker <live-market> --channels orderbook_delta --max-events 1`
  - `python -m mykalshi replay summary --session-dir <temp-dir>`
  - `python -m mykalshi backtest replay --session-dir <temp-dir> --strategy tests.cli_fixtures:DemoReplayStrategy`

## Follow-Up

- next roadmap slice should move back to richer market-family and settlement metadata for related-contract replay datasets
- after that, expand advanced live execution controls on top of the current trading workflow layer
