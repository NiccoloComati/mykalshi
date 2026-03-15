# Discovery Ergonomics Pass

Date: 2026-03-15

## Summary

This slice improves market discovery ergonomics so the repo can search by a generic query across series, events, and markets, and the higher-level workflow layer can expose grouped market universes instead of only flat market matches.

## Main Changes

- added generic `query` filtering to:
  - `discovery.search_series(...)`
  - `discovery.search_events(...)`
  - `discovery.search_markets(...)`
- added:
  - `discovery.resolve_series(...)`
  - `discovery.resolve_event(...)`
- kept early-stop paging behavior from the prior discovery-rate-limit pass
- expanded `research.workflows` with:
  - `DiscoveredSeries`
  - `DiscoveredEvent`
  - `MarketUniverse`
  - `ResearchSession.search_series(...)`
  - `ResearchSession.resolve_series(...)`
  - `ResearchSession.search_events(...)`
  - `ResearchSession.resolve_event(...)`
  - `ResearchSession.search_market_universes(...)`
- exported the new workflow surface through `mykalshi.research`
- added focused coverage in:
  - `tests/test_discovery.py`
  - `tests/test_research_workflows.py`

## Verification

- `python -m unittest tests.test_discovery tests.test_research_workflows -v`
- `python -m unittest discover -s tests -v`
- `python -m compileall mykalshi recorder_script.py`
- live read-only smoke checks passed for:
  - `discovery.search_markets(query="mars", status="open", limit=2)`
  - `ResearchSession.search_market_universes(query="mars", status="open", limit=3)`

## Follow-Up

- keep improving repeatable research workflows on top of the stronger discovery/session surface
- consider richer ranking/scoring for query matches rather than simple token containment
- after that, continue the roadmap into trading workflows and safety rails
