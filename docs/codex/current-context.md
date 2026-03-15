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
- `mykalshi/research/storage.py`: SQLite and Parquet sinks for captured order book events
- `mykalshi/research/backtest.py`: historical-trade backtest engine with target-position support
- `mykalshi/research/strategies.py`: reusable threshold and probability-edge strategies
- `mykalshi/research/engine/`: modular event-driven backtest engine components
- `mykalshi/research/datasets.py`: load and replay helpers for stored order book datasets
- `mykalshi/routing.py`: live/historical trade auto-routing
- `mykalshi/market.py`, `trading.py`, `exchange.py`, `events.py`, `communications.py`: endpoint wrappers

## Verified State

The foundation layer has been exercised in the local `.venv` on 2026-03-15.

- unit tests passed: `python -m unittest discover -s tests -v`
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

## Safety Note

The root `.env` currently resolves to the production Kalshi environment. Read-only checks are safe, but order placement, cancellation, amendment, and other account mutations must not be run without explicit intent.

## Next Implementation Slices

1. Migrate `TradeBacktester` compatibility flows onto the new event-driven engine.
2. Add cash and inventory reservation plus richer cancel/replace handling.
3. Improve queue-position and orderbook-based execution realism.

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
- `research.capture_market_data_sync(...)` is now the preferred entry point for live ticker/trade websocket collection.
- `routing.get_trades_auto(...)` is now the preferred entry point when code should not need to manually split live and archived trade sources.

## Working Conventions

- Keep changes in small commit-sized slices.
- Update `docs/codex/current-context.md` and add a matching note under `docs/codex/changes/` with each substantial commit.
- Prefer tests and safe dry runs before committing.
