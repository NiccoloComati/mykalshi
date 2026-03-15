# mykalshi

`mykalshi` is an opinionated Python wrapper around Kalshi's API aimed at four use cases:

- market and order book data collection
- research and exploratory analysis
- backtesting-oriented historical data access
- trading and portfolio operations

The repo started as a lightweight endpoint wrapper. This version reorganizes it into a safer library foundation:

- lazy config and auth loading instead of import-time side effects
- explicit demo vs production environments
- a reusable client core
- historical endpoint coverage for backtesting workflows
- a reusable order book recorder module instead of only a one-off script

## Install

Core package:

```bash
pip install .
```

Research/data stack:

```bash
pip install .[analysis,storage,websocket]
```

Or use the included `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Configuration

Supported environment variables:

- `KALSHI_ENV=production|demo`
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_REST_BASE_URL`
- `KALSHI_WS_URL`

Environment-specific values are also supported:

- `KALSHI_DEMO_API_KEY_ID`
- `KALSHI_DEMO_PRIVATE_KEY_PATH`
- `KALSHI_PROD_API_KEY_ID`
- `KALSHI_PROD_PRIVATE_KEY_PATH`

Legacy variables from the original repo are still supported:

- `ENV`
- `DEMO_KEYID`
- `DEMO_KEYFILE`
- `PROD_KEYID`
- `PROD_KEYFILE`

## Quick Start

```python
from mykalshi import discovery

matches = discovery.search_markets(series_ticker="KXELONMARS", status="open", limit=1)
print(matches[0]["market_ticker"])
```

```python
from mykalshi import historical

cutoff = historical.get_historical_cutoff()
trades = historical.get_historical_trades(ticker="INXD-25DEC31-T10000", limit=100)
```

```python
from mykalshi import KalshiClient, KalshiConfig

client = KalshiClient(KalshiConfig.from_env())
balance = client.get("/portfolio/balance", authenticated=True)
print(balance)
```

## Data Collection

Reusable recorder utilities live in `mykalshi.recorder`:

```python
from mykalshi.recorder import MarketLOBRecorder

recorder = MarketLOBRecorder(
    tickers=["KXBTC-YES", "KXBTC-NO"],
    interval_secs=5.0,
    output_path="lob_stream.jsonl",
)
recorder.start(duration_secs=60)
```

The top-level `recorder_script.py` now acts as a thin deployment script around that library code.

## Research Layer

Authenticated websocket capture lives under `mykalshi.research`:

```python
from mykalshi.research import KalshiWebsocketClient

client = KalshiWebsocketClient()
events = client.capture_orderbook_sync(
    "FED-23DEC-T3.00",
    max_events=10,
    duration_secs=30,
)
```

SQLite and Parquet sinks can be attached during capture:

```python
from mykalshi.research import MultiOrderbookSink, ParquetOrderbookSink, SQLiteOrderbookSink

sqlite_sink = SQLiteOrderbookSink("data/orderbook.sqlite")
parquet_sink = ParquetOrderbookSink("data/parquet")
sink = MultiOrderbookSink(sqlite_sink, parquet_sink)
```

Captured datasets can then be loaded and replayed:

```python
from mykalshi.research import load_orderbook_events, replay_orderbook_events

events = load_orderbook_events("data/orderbook.sqlite")
timeline = replay_orderbook_events(events)
```

Simple historical-trade backtests can now run without notebook CSV glue:

```python
from mykalshi.research import TradeBacktester, TradeSignal

def strategy(context, trade):
    if context.yes_position == 0:
        return TradeSignal("buy_yes", quantity=1)
    return None

result = TradeBacktester().run_on_historical_trades(
    "FED-23DEC-T3.00",
    strategy,
    initial_cash_cents=10000,
)
print(result.summary())
```

## Layout

- `mykalshi/client.py`: reusable HTTP client
- `mykalshi/config.py`: environment and credential loading
- `mykalshi/auth.py`: request signing helpers
- `mykalshi/discovery.py`: targeted series, event, and market discovery helpers
- `mykalshi/fixed_point.py`: Kalshi fixed-point conversion helpers
- `mykalshi/orderbook.py`: shared order book normalization/state helpers
- `mykalshi/historical.py`: historical data endpoints
- `mykalshi/recorder.py`: order book capture utilities
- `mykalshi/research/`: websocket capture, storage sinks, and backtest helpers
- `mykalshi/market.py`, `events.py`, `trading.py`, `communications.py`, `exchange.py`: endpoint wrappers

## Engineering Log

Persistent handoff notes for future Codex runs live under `docs/codex/`. The active state is in `docs/codex/current-context.md`, and commit-aligned notes live under `docs/codex/changes/`.

## Usage

Concrete workflows and runnable examples live in `USAGE.md`.

## Near-Term Roadmap

- richer typed models instead of raw dicts
- historical/live auto-routing helpers for backtests
- websocket client abstractions for order book delta replay
- dataset sinks for parquet, sqlite, and object storage
- a cleaner strategy/backtest layer on top of the raw API client
