# 2026-03-16 Faithful Main Notebook Port

## Summary

Rebuilt `notebooks/main_current.ipynb` from the original `notebooks/main.ipynb` structure instead of keeping the earlier reduced demo notebook.

## Restored Notebook Scope

The current notebook now mirrors the major sections and intent of the old notebook:

- preliminary market snapshot and open-market analysis
- presidential election event close-up
- aligned presidential candidate candlestick history
- lightweight trade sampling for the tested-election markets
- LOB polling and orderbook inspection flows
- climate and weather series exploration
- guarded IMDB appendix cells

## Toolkit Fixes Added For Notebook Compatibility

The faithful notebook port exposed several library issues which were fixed in the toolkit rather than patched around only inside the notebook:

- relative private-key paths from `.env` are now resolved from the dotenv location, so authenticated notebook calls work from `notebooks/`
- websocket sync wrappers now work inside a running notebook event loop
- SQLite sinks now support background-thread writes used by the notebook-safe websocket wrapper
- `market.get_market_orderbook(...)` now adds a legacy-compatible `orderbook` view alongside `orderbook_fp`
- candlestick helpers now normalize current fixed-point and dollar-denominated candle payloads into the old notebook-friendly columns
- `events.event_info(...)` now normalizes current market payload fields into the legacy notebook table shape
- lightweight trade preview helpers were added in `routing.py` to avoid expensive full-history pulls for notebook inspection cells

## Verification

- `python scripts/build_main_current_notebook.py`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- live notebook-oriented smoke checks passed for:
  - `events.event_info("PRES-2024")`
  - `market.get_full_market(series_ticker="PRES", ...)`
  - `routing.get_trades_preview_dataframe_auto("PRES-2024-DJT", limit=5)`
  - authenticated `market.get_market_orderbook(...)` from the `notebooks/` working directory
  - `ResearchSession.capture_market_session(...)` inside a running event loop
