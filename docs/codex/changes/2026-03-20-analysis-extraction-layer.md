# 2026-03-20 Analysis Extraction Layer

## Summary

Promoted the most reusable notebook analysis patterns into first-class research modules.

## Added

- `mykalshi/research/event_analysis.py`
  - `load_market_history(...)`
  - `build_market_comparison_panel(...)`
  - `build_event_closeup(...)`
  - `plot_event_closeup(...)`
- `mykalshi/research/charts.py`
  - `plot_market_candles(...)`
  - `plot_market_comparison(...)`
- `mykalshi/research/orderbook_analysis.py`
  - `get_orderbook_snapshot(...)`
  - `render_orderbook_text(...)`
  - `plot_orderbook_depth(...)`
  - `orderbook_snapshots_to_matrices(...)`
  - `plot_orderbook_matrix_snapshot(...)`
- `mykalshi/research/family_analysis.py`
  - `build_market_family_analysis(...)`
  - `plot_market_family_comparison(...)`
- `mykalshi/research/universe.py`
  - `UniverseSpec`
  - `resolve_market_universe(...)`
  - `sync_market_universe(...)`
  - `open_market_universe(...)`

## Integration

- exported the new analysis helpers from `mykalshi.research`
- added `ResearchSession` wrappers for:
  - event closeups
  - market-family analysis
  - order book snapshots
  - filtered market-universe sync/open

## Verification

- `python -m unittest tests.test_research_analysis -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py scripts/build_main_current_notebook.py`

## Notes

- these extractions are based on notebook intent, not a direct copy of notebook code
- the order-book matrix and family-analysis paths were rewritten into reusable library code rather than preserving the older ad hoc notebook implementations
