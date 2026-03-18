# 2026-03-18 Market Snapshot Anchor Sync

## Summary

Implemented anchored market-snapshot refresh support and wired the notebook snapshot loader onto it.

## What Changed

- added `market.sync_market_snapshot_csv(...)` to maintain a cached market snapshot plus a sidecar anchor JSON file
- added `market.default_market_snapshot_anchor_path(...)` for the default anchor-file location
- expanded `market.get_all_markets(...)` so incremental filters such as `min_updated_ts` pass through to paginated market collection
- updated `notebooks/main_current.ipynb` generation so `load_market_snapshot()` now:
  - picks the latest snapshot or creates a new timestamped one
  - refreshes it incrementally through `market.sync_market_snapshot_csv(...)`
  - prints the refresh mode and delta count before loading the CSV

## Anchor Behavior

- if no snapshot exists, a full snapshot is downloaded and an anchor file is created
- if a snapshot exists with an anchor file, the anchor timestamp is used as `min_updated_ts`
- if a snapshot exists without an anchor file, the sync bootstraps from the timestamp encoded in the snapshot filename, falling back to file mtime when needed
- incremental updates merge by `ticker`, replacing updated rows and appending new rows

## Verification

- `python -m unittest tests.test_market_snapshot -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py scripts/build_main_current_notebook.py`
