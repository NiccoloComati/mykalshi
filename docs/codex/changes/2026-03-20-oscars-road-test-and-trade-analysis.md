# 2026-03-20: Oscars Road Test And Trade Analysis

## Summary

Added a reusable trade-analysis layer, hardened event-level market loading for Kalshi's inconsistent nested-market responses, and generated a concrete Oscars road-test report using the current research stack.

## Code Changes

- Added `mykalshi/research/trade_analysis.py`:
  - `TradeHistory`
  - `load_trade_history(...)`
  - `summarize_trade_history(...)`
  - `resample_trade_history(...)`
  - `plot_trade_activity(...)`
- Updated `mykalshi/research/event_analysis.py` with `load_event_market_payload(...)` so event-closeup flows can fall back from direct event lookup to the series-scoped event listing when nested markets come back empty.
- Updated `mykalshi/formatting.py` so `parse_timestamp(...)` accepts ISO-8601 strings.
- Wired the new analysis helpers through:
  - `mykalshi/research/__init__.py`
  - `mykalshi/research/workflows.py`
- Added:
  - `scripts/analyze_oscars_dynamics.py`
  - `tests/test_research_trade_analysis.py`
  - `tests/test_formatting.py`

## Analysis Artifact

Generated a live report under `docs/analysis/oscars-2026-road-test/` with:

- cross-category winner/favorite summary
- favorite-gap and winner-volume plots
- winner comparison plot for the final 30 days
- deep dives for Best Picture, Best Actor, and Best Actress
- trade-activity plots for those winners
- a live orderbook depth check on the next Oscars cycle

## Validation

- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py scripts/analyze_oscars_dynamics.py scripts/build_main_current_notebook.py`
- live report generation:
  - `python scripts/analyze_oscars_dynamics.py --as-of 2026-03-20T12:00:00+00:00`

## Notes

- The report intentionally distinguishes between what the toolkit can do now and what still requires prior capture. Historical Oscar trades and quote candles are available; historical orderbook microstructure is not reconstructible unless websocket/orderbook capture was running at the time.
