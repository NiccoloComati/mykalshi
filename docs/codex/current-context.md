# Current Context

Last updated: 2026-03-15

## Repo Goal

Turn `mykalshi` into a clean research and trading toolkit for Kalshi with four solid use cases:

- live and historical market data access
- market microstructure data collection
- backtesting and research workflows
- safer trading utilities

## Current Architecture

- `mykalshi/config.py`: environment and credential loading
- `mykalshi/auth.py`: request signing
- `mykalshi/client.py`: reusable HTTP client
- `mykalshi/discovery.py`: high-level series, event, and market discovery
- `mykalshi/fixed_point.py`: shared fixed-point conversion helpers
- `mykalshi/orderbook.py`: shared order book normalization and state tracking
- `mykalshi/transport.py`: thin wrapper helpers plus cursor pagination
- `mykalshi/historical.py`: historical endpoints for research and backtesting
- `mykalshi/recorder.py`: reusable polling-based order book recorder
- `mykalshi/research/websocket.py`: authenticated websocket capture for order book snapshots and deltas
- `mykalshi/research/storage.py`: SQLite and Parquet sinks for captured market-data and orderbook events, plus split-routing helpers for standardized capture sessions
- `mykalshi/research/backtest.py`: legacy-compatible trade backtester plus first-class replay backtester over captured datasets
- `mykalshi/research/strategies.py`: reusable threshold and probability-edge strategies
- `mykalshi/research/engine/`: modular event-driven backtest engine components
- `mykalshi/research/datasets.py`: load and replay helpers for stored market-data and orderbook datasets
- `mykalshi/routing.py`: live/historical trade auto-routing
- `mykalshi/trading_workflows.py`: higher-level trading state snapshots, execution helpers, and safety rails
- `mykalshi/cli.py`: user-facing CLI over discovery, capture, replay, backtest, and trading workflows
- `mykalshi/market.py`, `trading.py`, `exchange.py`, `events.py`, `communications.py`: endpoint wrappers

## Verified State

The foundation layer has been exercised in the local `.venv` on 2026-03-15.

- unit tests passed: `python -m unittest discover -s tests -v`
- focused backtest execution-realism tests passed: `python -m unittest tests.test_research_event_engine tests.test_research_backtest -v`
- import check passed: `import mykalshi`
- compile check passed: `python -m compileall mykalshi recorder_script.py`
- live read-only REST checks passed:
  - exchange status
  - historical cutoff
  - open markets
  - portfolio balance
  - orders
  - historical orders
  - authenticated order book
- live authenticated websocket handshake passed against `wss://api.elections.kalshi.com/trade-api/ws/v2`
- live orderbook capture through `KalshiWebsocketClient.capture_orderbook_sync(...)` passed
- polling recorder compatibility was updated for the current REST `orderbook_fp` response shape
- live websocket capture into both SQLite and Parquet sinks passed
- live historical-trade backtest path passed through `TradeBacktester.run_on_historical_trades(...)`
- a user-facing workflow guide now exists in `USAGE.md`
- live discovery queries passed for series search, market search, and exact market resolution
- live capture-to-SQLite load/replay round-trip passed
- live historical backtest passed with Kalshi-style taker fees and limit-order handling
- live ticker and trade websocket capture passed through the generic market-data path
- live generic market-data storage round-trip passed for SQLite and Parquet
- live auto-routing checks passed for one archived ticker and one live ticker
- live archived-data dry runs passed for:
  - `ProbabilityEdgeStrategy`
  - `ThresholdSignalStrategy`
  - `PositionTargetSignal` factory helpers
- live event-driven engine dry runs passed for:
  - archived trade replay through `HistoricalTradeReplay`
  - live ticker replay through `MarketDataReplay`
- live historical wrapper dry run passed through `TradeBacktester.run_on_historical_trades(...)` after the compatibility migration
- local replay-wrapper dry runs passed through `ReplayBacktester.run_on_replay_event_stream(...)` and `run_on_captured_dataset(...)`
- local replay-lifecycle enrichment dry runs passed through `ReplayBacktester.enrich_replay_event_stream(...)`
- local reporting/export dry runs passed through:
  - `BacktestRunResult.market_summaries()`
  - `BacktestRunResult.position(...)`
  - `BacktestRunResult.to_dataframes()`
- local workflow/session dry runs passed through:
  - `ResearchSession.load_replay_dataset(...)`
  - `ReplayDataset.backtest(...)`
  - `ResearchSession.run_replay_backtest(...)`
  - `ResearchSession.capture_market_session(...)`
  - `ResearchSession.open_capture_session(...)`
- live rate-limit checks passed through:
  - `trading.get_account_limits()` returning `read_limit=20`, `write_limit=10`, `usage_tier=basic`
  - `ResearchSession.search_markets(status="open", limit=1)` after the discovery paging fix
- live discovery-ergonomics checks passed through:
  - `discovery.search_markets(query="mars", status="open", limit=2)`
  - `ResearchSession.search_market_universes(query="mars", status="open", limit=3)`
- live trading-workflow checks passed through:
  - `TradingSession.snapshot(order_status="resting")`
  - `TradingSession.market_snapshot(...)`
  - `TradingSession.buy_yes(..., dry_run=True)`
  - production write blocking via `TradingSession.buy_yes(...)`
  - `TradingSession.cancel_stale_orders(..., dry_run=True)`
- live CLI checks passed through:
  - `python -m mykalshi discover markets --status open --limit 1`
  - `python -m mykalshi trading snapshot`
  - `python -m mykalshi capture market-data --channels ticker --market-ticker ... --send-initial-snapshot --max-events 1`
  - `python -m mykalshi capture session <temp-dir> --market-ticker <live-market> --channels orderbook_delta --max-events 1`
  - `python -m mykalshi replay summary --session-dir <temp-dir>`
  - `python -m mykalshi backtest replay --session-dir <temp-dir> --strategy tests.cli_fixtures:DemoReplayStrategy`
  - `python -m mykalshi trading plan-order ...` in dry-run mode
  - `python -m mykalshi backtest historical ... --strategy tests.cli_fixtures:historical_strategy`
  - installed console script `mykalshi --help`

## Safety Note

The root `.env` currently resolves to the production Kalshi environment. Read-only checks are safe, but order placement, cancellation, amendment, and other account mutations must not be run without explicit intent.

## Next Implementation Slices

1. Add richer market-family and settlement metadata handling where replayed datasets span related contracts.
2. Extend the trading workflow layer with more advanced execution controls on top of the current safety rails.
3. Keep tightening the CLI and examples around end-to-end research sessions now that capture, replay, and replay-backtest share one session format.

## Recent Usability Notes

- Literal placeholder strings like `"YOUR_TICKER"` are not valid inputs and can cause 404s or long waits.
- For quick websocket smoke tests, use a real market ticker and `max_events=1`.
- `market.*` and `historical.*` are intentionally separate because Kalshi splits live/recent data from archived data.
- `tar_flow.py` and `his_flow.py` are now runnable local smoke-test scripts.
- `discovery.*` is now the preferred starting point when the user wants to target a specific series, event, or market rather than pull arbitrary markets.
- `research.backtest` now has a more realistic engine shape: orders, fills, fee models, and limit-price rejection.
- `research.backtest` now also supports target-position signals, staged transitions, and rejection-on-risk instead of aborting the run.
- `research.strategies` is now the preferred place for reusable signal-to-position logic.
- `research.engine` is now the new backtest foundation for deterministic event-driven replay.
- `research.backtest` now routes through `research.engine` instead of maintaining a separate procedural simulator.
- the event-driven engine now has cash and inventory reservation, explicit cancel/replace state transitions, and a centralized settlement pipeline.
- the event-driven engine now has pluggable fill models with immediate-compatibility and orderbook-aware queue-sensitive behavior, plus explicit aggressive/passive liquidity-role metadata on fills.
- the event-driven engine now routes `liquidity_role` into fee-model callbacks and supports maker/taker-aware fee modeling while preserving legacy fee signatures.
- queue-ahead depletion now also uses orderbook level-size drops and ticker top-of-book size drops (when available) in addition to trade prints.
- strategies can now set per-order `latency_events` to delay fill eligibility by replay events for deterministic latency simulation.
- `research.datasets` now provides merged replay helpers so stored ticker/trade and reconstructed orderbook streams can be loaded into one ordered event timeline for engine replay.
- `research.ReplayBacktester` is now the preferred high-level entry point for backtests over stored replay datasets.
- `research.ReplayBacktester` now enriches replay timelines with synthetic expiration/settlement events from Kalshi market metadata when replayed data is incomplete.
- `research.engine.BacktestRunResult` now exposes position snapshots, per-market summaries, turnover/exposure metrics, and dataframe export helpers.
- `research.workflows` now provides `DiscoveredMarket`, `ReplayDataset`, and `ResearchSession` so discovery, dataset loading, and replay/historical backtests can be driven through one higher-level API.
- `research.workflows` now also provides `CaptureSession` plus standardized session manifests so websocket capture, replay inspection, and replay backtests can share one directory-based workflow.
- `research.workflows.ResearchSession.capture_market_session(...)` can now capture directly from an explicit `market_ticker` or by resolving one from discovery filters.
- `research.capture_market_data_sync(...)` is now the preferred entry point for live ticker/trade websocket collection.
- `routing.get_trades_auto(...)` is now the preferred entry point when code should not need to manually split live and archived trade sources.
- `client.KalshiClient` now has centralized account-aware rate limiting, `429` retry handling, and `/account/limits` auto-detection.
- `discovery.*` now stops paging once search limits are satisfied instead of fetching full result sets and truncating afterward.
- `discovery.*` now supports a generic `query` path across series/event/market context plus `resolve_series(...)` and `resolve_event(...)`.
- `discovery.search_markets(...)` now normalizes response-style market statuses like `active` into the Kalshi filter values the REST API accepts, so users can reuse observed market statuses in discovery and session capture flows.
- `research.workflows.ResearchSession` now wraps series/event discovery and grouped market-universe browsing in addition to replay datasets and backtests.
- `trading_workflows.TradingSession` is now the preferred higher-level entry point for coherent account state snapshots and safer live-order workflows.
- `trading_workflows.TradingSafetyPolicy` now provides dry-run mode, production write blocking, order-size/risk caps, open-order caps, allowed/blocked ticker controls, and JSONL audit logging.
- `trading_workflows` now provides higher-level helpers for market context, order submission, amend, replace, stale-order cancellation, and position flattening.
- `cli.py` now provides a real `mykalshi` command surface over discovery, websocket capture, replay inspection, historical/replay backtests, and trading workflow helpers.
- the CLI now supports standardized `capture session` directories plus `--session-dir` for replay summary and replay backtests.
- the CLI strategy loader accepts Python import paths like `module.submodule:ClassName` or `module.submodule:function_name`.
- trading mutation commands in the CLI default to dry-run planning and require `--execute` for live writes.

## Working Conventions

- Keep changes in small commit-sized slices.
- Update `docs/codex/current-context.md` and add a matching note under `docs/codex/changes/` with each substantial commit.
- Prefer tests and safe dry runs before committing.
