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
- `mykalshi/fixed_point.py`: shared fixed-point conversion helpers
- `mykalshi/orderbook.py`: shared order book normalization and state tracking
- `mykalshi/transport.py`: thin wrapper helpers plus cursor pagination
- `mykalshi/historical.py`: historical endpoints for research and backtesting
- `mykalshi/recorder.py`: reusable polling-based order book recorder
- `mykalshi/research/websocket.py`: authenticated websocket capture for order book snapshots and deltas
- `mykalshi/research/storage.py`: SQLite and Parquet sinks for captured order book events
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

## Safety Note

The root `.env` currently resolves to the production Kalshi environment. Read-only checks are safe, but order placement, cancellation, amendment, and other account mutations must not be run without explicit intent.

## Next Implementation Slices

1. A simple backtest API that uses `mykalshi.historical` and stored datasets.
2. Higher-level research helpers for replay and dataset loading.
3. Broader channel coverage beyond orderbook capture.

## Working Conventions

- Keep changes in small commit-sized slices.
- Update `docs/codex/current-context.md` and add a matching note under `docs/codex/changes/` with each substantial commit.
- Prefer tests and safe dry runs before committing.
