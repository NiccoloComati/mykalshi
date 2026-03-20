# 2026-03-20 Market Snapshot Memory Fix

## Summary

Fixed the `load_market_snapshot()` notebook path so it no longer explodes memory usage on the large cached market snapshot.

## Changes

- refactored `market.sync_market_snapshot_csv(...)` to stream full-refresh and incremental market rows through a temporary SQLite staging file instead of materializing the full snapshot merge in pandas
- kept anchored delta refresh behavior, but made the merge path operate row-by-row against the existing CSV
- updated `scripts/build_main_current_notebook.py` so `load_market_snapshot()` only reads the subset of market columns actually used by the notebook
- rebuilt `notebooks/main_current.ipynb` from the generator
- updated `tests/test_market_snapshot.py` to cover the new streaming snapshot path

## Verification

- `python -m unittest tests.test_market_snapshot -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py scripts/build_main_current_notebook.py`
- direct smoke read on `all_markets_2025-07-11-21-30-55.csv`:
  - 2,140,645 rows
  - 19 columns loaded
  - about 674 MB pandas memory footprint
