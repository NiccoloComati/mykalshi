# Rate Limit And Discovery Paging

Date: 2026-03-15

## Summary

This slice fixes the concrete causes behind Kalshi `429 too_many_requests` responses in normal usage by adding centralized client-side pacing and by stopping discovery helpers from over-fetching paginated datasets when the caller only asked for a small limit.

## Main Changes

- added `mykalshi/rate_limit.py`
- added centralized read/write pacing in `mykalshi/client.py`
- added `/account/limits` auto-detection in `KalshiClient`
- added `429` retry handling with `Retry-After` support in `KalshiClient`
- added rate-limit config flags and overrides in `mykalshi/config.py`
- added authenticated `trading.get_account_limits()`
- added `max_items` support in `collect_cursor_pages(...)`
- updated `discovery.py` to page incrementally and stop once `limit` is satisfied instead of fetching full datasets first
- updated `events.py` and `market.py` pagination helpers to support early stopping
- added focused coverage in:
  - `tests/test_client.py`
  - `tests/test_config.py`
  - `tests/test_discovery.py`
  - `tests/test_wrappers.py`

## Verification

- `python -m unittest tests.test_client tests.test_config tests.test_discovery tests.test_wrappers -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- live read-only checks passed for:
  - `trading.get_account_limits()`
  - `ResearchSession.search_markets(status="open", limit=1)`

## Follow-Up

- continue improving higher-level research ergonomics and repeatable workflows
- add richer discovery ergonomics around category/subdomain semantics
- keep trading-layer safety rails as the next major roadmap area after research ergonomics
