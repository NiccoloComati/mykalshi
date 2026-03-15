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

The package now installs a `mykalshi` console command:

```bash
mykalshi --help
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
- `KALSHI_ENABLE_RATE_LIMITING`
- `KALSHI_AUTO_DETECT_ACCOUNT_LIMITS`
- `KALSHI_READ_LIMIT_PER_SECOND`
- `KALSHI_WRITE_LIMIT_PER_SECOND`
- `KALSHI_ACCOUNT_LIMITS_CACHE_SECONDS`
- `KALSHI_MAX_RATE_LIMIT_RETRIES`

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

The client now paces requests centrally and can auto-detect your current Kalshi account limits from `/account/limits`, so normal wrapper and discovery usage should avoid preventable `429` bursts.

There is now also a higher-level trading workflow layer in `mykalshi.trading_workflows` for coherent account snapshots, dry-run execution planning, stale-order cleanup, and production write guards.

## Quick Start

```python
from mykalshi.research import ResearchSession

session = ResearchSession()
matches = session.search_markets(series_ticker="KXELONMARS", status="open", limit=1)
print(matches[0].market_ticker)
```

The same session layer now supports broader query-style discovery:

```python
universes = session.search_market_universes(query="mars", status="open", limit=3)
print(universes[0].summary())
```

There is now also a CLI over discovery, capture, replay, backtest, and trading workflows:

```bash
mykalshi discover markets --query "elon mars" --status open --limit 3
mykalshi trading snapshot
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

```python
from mykalshi.trading_workflows import TradingSafetyPolicy, TradingSession

session = TradingSession(policy=TradingSafetyPolicy(dry_run=True))
snapshot = session.snapshot(order_status="resting")
print(snapshot.summary())
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

Broader market-data capture is also available for `ticker` and `trade` channels:

```python
from mykalshi.research import KalshiWebsocketClient

client = KalshiWebsocketClient()
events = client.capture_market_data_sync(
    channels=["ticker", "trade"],
    market_ticker="KXELONMARS-99",
    send_initial_snapshot=True,
    max_events=2,
    duration_secs=10,
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
from mykalshi.research import load_market_data_events, load_orderbook_events, replay_orderbook_events

market_data = load_market_data_events("data/market-data.sqlite")
events = load_orderbook_events("data/orderbook.sqlite")
timeline = replay_orderbook_events(events)
```

Merged replay timelines can also be backtested directly with the higher-level replay wrapper:

```python
from mykalshi.research import KalshiStrategy, ReplayBacktester

class DemoReplayStrategy(KalshiStrategy):
    def __init__(self):
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        self.submitted = True

result = ReplayBacktester().run_on_captured_dataset(
    DemoReplayStrategy(),
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
    initial_cash_cents=10000,
)
print(result.summary())
```

Replay backtests now enrich missing expiration and settlement steps from Kalshi market metadata by default, and engine results expose richer analytics:

```python
print(result.market_summary("KXELONMARS-99").summary())
print(result.position("KXELONMARS-99").summary())

frames = result.to_dataframes()
print(frames["fills"].head())
print(frames["markets"].head())
```

If you want a higher-level research path that avoids manual discovery/dataset/backtest glue, use `ResearchSession`:

```python
from mykalshi.research import ResearchSession, KalshiStrategy

class DemoReplayStrategy(KalshiStrategy):
    def __init__(self):
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        self.submitted = True

session = ResearchSession()
dataset = session.load_replay_dataset(
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
)
print(dataset.summary())

result = dataset.backtest(DemoReplayStrategy(), initial_cash_cents=10000)
print(result.summary())
```

The same workflow layer now supports standardized capture-session directories so websocket capture, replay inspection, and replay backtests can share one stored session:

```python
match = session.resolve_market(query="mars", status="open")
capture = session.capture_market_session(
    "data/sessions/mars",
    market_ticker=match.market_ticker,
    max_events=1,
)
print(capture.summary())

dataset = session.load_replay_dataset(session_dir="data/sessions/mars")
result = session.run_replay_backtest(
    DemoReplayStrategy(),
    session_dir="data/sessions/mars",
    initial_cash_cents=10000,
)
print(result.summary())
```

You can also resolve the market inside `capture_market_session(...)` by passing discovery filters like `query=...` and `status=...`.

Trade history can also be auto-routed across live and archived sources:

```python
from mykalshi import routing

trades = routing.get_trades_auto("KXELONMARS-99")
print(trades["sources_used"])
```

Simple historical-trade backtests can now run without notebook CSV glue:

```python
from mykalshi import historical
from mykalshi.research import ProbabilityEdgeStrategy, TradeBacktester

sample_trade = historical.get_historical_trades(limit=1)["trades"][0]
ticker = sample_trade["ticker"]

strategy = ProbabilityEdgeStrategy(
    probability_fn=lambda context, trade: "0.58",
    enter_edge_cents=12,
    exit_edge_cents=4,
    target_quantity=1,
)

result = TradeBacktester().run_on_historical_trades(
    ticker,
    strategy,
    initial_cash_cents=10000,
)
print(result.summary())
```

There is also a lower-level event-driven engine for more realistic replay flows:

```python
from mykalshi.research import EventDrivenBacktestEngine, HistoricalTradeReplay, KalshiStrategy

class DemoStrategy(KalshiStrategy):
    def on_trade(self, context, event):
        if event.yes_price_cents <= 40 and not context.open_orders(event.market_ticker):
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=41)

engine = EventDrivenBacktestEngine(initial_cash_cents=10000)
result = engine.run(HistoricalTradeReplay.from_trade_dicts(trades), DemoStrategy())
print(result.summary())
```

The CLI now exposes the same session-based flow:

```bash
mykalshi capture session data/sessions/mars --query "mars" --status open --channels orderbook_delta --max-events 1
mykalshi replay summary --session-dir data/sessions/mars
mykalshi backtest replay --session-dir data/sessions/mars --strategy tests.cli_fixtures:DemoReplayStrategy
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
- `mykalshi/research/`: websocket capture, storage sinks, backtest helpers, and reusable strategies
- `mykalshi/research/engine/`: event-driven backtest engine components
- `mykalshi/routing.py`: live/historical auto-routing helpers
- `mykalshi/trading_workflows.py`: higher-level trading state snapshots, execution helpers, and safety rails
- `mykalshi/market.py`, `events.py`, `trading.py`, `communications.py`, `exchange.py`: endpoint wrappers

## Engineering Log

Persistent handoff notes for future Codex runs live under `docs/codex/`. The active state is in `docs/codex/current-context.md`, and commit-aligned notes live under `docs/codex/changes/`.

## Usage

Concrete workflows and runnable examples live in `USAGE.md`.

The engine design note for this refactor lives in `docs/backtest-engine-architecture.md`.

## Near-Term Roadmap

- richer typed models instead of raw dicts
- higher-level research ergonomics around discovery, load, replay, and backtest workflows
- richer market-family and settlement metadata for related-contract replay datasets
- more polished end-to-end CLI workflows and examples on top of the now-stronger research and trading layers
