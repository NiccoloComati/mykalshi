# 2026-03-15 Main Notebook Current API

## Summary

Added `notebooks/main_current.ipynb` as a current-code replacement for the original exploratory `notebooks/main.ipynb`.

## Notebook Scope

The rebuilt notebook now demonstrates:

- live open-market overview
- event and market drilldown
- bounded candlestick retrieval on the current API
- orderbook visualization using the current `orderbook_fp` response shape
- standardized replay-session capture
- replay backtest on captured data
- climate and weather discovery with the current discovery layer

## Follow-On Fixes

The notebook smoke run surfaced one live-data compatibility issue in the library:

- `DiscoveredSeries.from_discovery_result(...)` now handles `tags=None`

## Verification

- JSON validation passed for `notebooks/main_current.ipynb`
- notebook code path was executed as a plain Python smoke script because `jupyter` is not installed in the environment
- smoke outputs confirmed:
  - open market sampling
  - live event/market selection
  - candlestick retrieval
  - current orderbook plotting path
  - session capture
  - replay backtest
  - climate/weather discovery
