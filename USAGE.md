# Usage Guide

This repo is currently a Python toolkit, not a standalone app.

The main workflows that are usable today are:

- live market discovery and data pulls
- archived historical data pulls
- websocket orderbook capture
- SQLite and Parquet dataset writing
- historical-trade and replay-dataset backtests

## 1. Setup

Activate the local virtual environment and install editable dependencies:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -e .[analysis,storage,websocket]
```

Your root `.env` currently resolves to production, so authenticated calls hit the live account unless you switch `KALSHI_ENV=demo`.

The client now rate-limits requests centrally. By default it also auto-detects your account's current Kalshi `read_limit` and `write_limit` from `/account/limits`.

## 2. Import Surface

The main modules are:

```python
from mykalshi import market, historical, events, trading
from mykalshi.research import (
    DiscoveredMarket,
    EventDrivenBacktestEngine,
    HistoricalTradeReplay,
    KalshiWebsocketClient,
    KalshiStrategy,
    ReplayDataset,
    MultiOrderbookSink,
    ParquetOrderbookSink,
    PositionTargetSignal,
    ProbabilityEdgeStrategy,
    ReplayBacktester,
    ResearchSession,
    SQLiteOrderbookSink,
    ThresholdSignalStrategy,
    TradeBacktester,
    TradeSignal,
)
```

## 3. Live Market Discovery

Do not use literal placeholder strings like `"YOUR_TICKER"`. Pull a real market first.

The higher-level helper for this is `mykalshi.discovery`.

If you want a single user-facing entry point for discovery plus replay/backtest workflows, start with `ResearchSession`:

```python
from mykalshi.research import ResearchSession

session = ResearchSession()
matches = session.search_markets(series_ticker="HIGHMIA", status="open", limit=3)
print(matches[0].summary())
```

You can also use one generic query across series, event, and market context:

```python
session = ResearchSession()
matches = session.search_markets(query="elon mars", status="open", limit=5)
print(matches[0].summary())
```

### By series

```python
from mykalshi import events

series = events.get_series_list()["series"][0]
print(series["ticker"], series["title"])
```

### By event inside a series

```python
from mykalshi import events

event = events.get_events(series_ticker="HIGHMIA", status="open")["events"][0]
print(event["event_ticker"], event["title"])
```

### Markets inside an event

```python
from mykalshi import events, market

event = events.get_events(limit=1)["events"][0]
markets = market.get_markets(event_ticker=event["event_ticker"], limit=10)
for item in markets["markets"]:
    print(item["ticker"], item["title"], item["status"])
```

### Quick smoke-test market

```python
from mykalshi import market

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]
print(ticker)
```

### Targeted discovery by series, event, or title

```python
from mykalshi import discovery

markets = discovery.search_markets(
    series_ticker="KXELONMARS",
    status="open",
    limit=5,
)
print(markets[0]["market_ticker"])
```

```python
from mykalshi import discovery

match = discovery.resolve_market(
    event_ticker="KXELONMARS-99",
    market_ticker_contains="KXELONMARS-99",
)
print(match["market_ticker"])
```

The session wrapper exposes the same discovery path but returns typed `DiscoveredMarket` objects:

```python
from mykalshi.research import ResearchSession

session = ResearchSession()
match = session.resolve_market(
    event_ticker="KXELONMARS-99",
    market_ticker_contains="KXELONMARS-99",
)
print(match.market_ticker)
```

There are now matching helpers for higher-level discovery objects too:

```python
series_match = session.resolve_series(query="elon mars")
event_match = session.resolve_event(query="elon mars 2099")
print(series_match.summary())
print(event_match.summary())
```

And if you want grouped browsing by event instead of a flat market list:

```python
universes = session.search_market_universes(query="mars", status="open", limit=5)
print(universes[0].summary())
print([market.market_ticker for market in universes[0].markets])
```

## 4. Live Market Data

### Market metadata

```python
from mykalshi import market

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]
print(market.get_market(ticker))
```

### Order book

```python
from mykalshi import market

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]
print(market.get_market_orderbook(ticker))
```

### Recent live trades

```python
from mykalshi import market

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]
trades = market.get_trades(ticker=ticker, limit=20)
print(trades["trades"])
```

## 5. Historical Data

This is the key distinction:

- `market.*` is for live/recent exchange data
- `historical.*` is for archived data behind Kalshi's historical cutoff

Inspect the cutoff first:

```python
from mykalshi import historical

print(historical.get_historical_cutoff())
```

Get a real archived ticker before requesting archived trades:

```python
from mykalshi import historical

sample_trade = historical.get_historical_trades(limit=1)["trades"][0]
ticker = sample_trade["ticker"]
archived_trades = historical.get_historical_trades(ticker=ticker, limit=20)
print(ticker)
print(archived_trades["trades"])
```

If you pass a fake ticker, or a ticker that does not exist in archived data, Kalshi can return `404`.

## 6. Websocket Orderbook Capture

For a quick smoke test, capture only one event so the call returns immediately after the initial snapshot:

```python
from mykalshi import market
from mykalshi.research import KalshiWebsocketClient

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]
client = KalshiWebsocketClient()
events = client.capture_orderbook_sync(
    ticker,
    max_events=1,
    duration_secs=10,
)
print(events[0])
```

If you ask for `max_events=10`, the call waits for deltas after the initial snapshot. Some markets will be quiet, so that can take a while.

## 7. Capture Into SQLite And Parquet

```python
from mykalshi import market
from mykalshi.research import (
    KalshiWebsocketClient,
    SQLiteOrderbookSink,
    ParquetOrderbookSink,
    MultiOrderbookSink,
)

ticker = market.get_markets(limit=1, status="open")["markets"][0]["ticker"]

sqlite_sink = SQLiteOrderbookSink("data/orderbook.sqlite")
parquet_sink = ParquetOrderbookSink("data/parquet")
sink = MultiOrderbookSink(sqlite_sink, parquet_sink)

client = KalshiWebsocketClient()
events = client.capture_orderbook_sync(
    ticker,
    sink=sink,
    max_events=1,
    duration_secs=10,
)

sqlite_sink.close()
parquet_sink.close()
print(len(events))
```

## 8. Capture Ticker And Trade Streams

```python
from mykalshi.research import (
    KalshiWebsocketClient,
    MultiMarketDataSink,
    ParquetMarketDataSink,
    SQLiteMarketDataSink,
)

sqlite_sink = SQLiteMarketDataSink("data/market-data.sqlite")
parquet_sink = ParquetMarketDataSink("data/market-data-parquet")
sink = MultiMarketDataSink(sqlite_sink, parquet_sink)

client = KalshiWebsocketClient()
ticker_events = client.capture_market_data_sync(
    channels=["ticker"],
    market_ticker="KXELONMARS-99",
    send_initial_snapshot=True,
    max_events=1,
    duration_secs=10,
    sink=sink,
)

trade_events = client.capture_market_data_sync(
    channels=["trade"],
    max_events=1,
    duration_secs=10,
    sink=sink,
)

sqlite_sink.close()
parquet_sink.close()
print(ticker_events[0]["channel"], trade_events[0]["channel"])
```

## 9. Load And Replay Stored Orderbook Data

```python
from mykalshi.research import load_market_data_events, load_orderbook_events, replay_orderbook_events

market_data = load_market_data_events("data/market-data.sqlite")
events = load_orderbook_events("data/orderbook.sqlite")
timeline = replay_orderbook_events(events)
print(market_data[0]["channel"])
print(timeline[0]["event_type"])
```

## 10. Backtesting On Archived Trades

Use a real archived ticker, not a placeholder.

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

Current backtest engine features include:

- strategy callbacks
- target-position signals
- limit-price support
- explicit rejected vs filled orders
- pluggable fee models
- a Kalshi-style taker fee model
- mark-to-market summaries with drawdown

You can also emit position targets directly:

```python
from mykalshi.research import PositionTargetSignal, TradeBacktester

def strategy(context, trade):
    return PositionTargetSignal(
        "yes",
        target_quantity=2,
        max_trade_quantity=1,
    )
```

Or use a generic threshold strategy around any numeric score:

```python
from mykalshi.research import ThresholdSignalStrategy

strategy = ThresholdSignalStrategy(
    signal_fn=lambda context, trade: trade["my_score"],
    yes_threshold=0.50,
    no_threshold=-0.50,
    target_quantity=1,
)
```

## 10A. Event-Driven Backtests

The newer engine path is event-driven and keeps replay, order handling, fills, portfolio state, and reporting separate.

```python
from mykalshi.research import EventDrivenBacktestEngine, HistoricalTradeReplay, KalshiStrategy

class DemoStrategy(KalshiStrategy):
    def on_trade(self, context, event):
        if event.yes_price_cents <= 40 and not context.open_orders(event.market_ticker):
            context.buy_yes(event.market_ticker, quantity=1, limit_price_cents=41)

engine = EventDrivenBacktestEngine(initial_cash_cents=10000)
replay = HistoricalTradeReplay.from_trade_dicts(archived_trades)
result = engine.run(replay, DemoStrategy())
print(result.summary())
```

The strategy context exposes:

- `market(...)`
- `position(...)`
- `open_orders(...)`
- `buy_yes(...)`, `sell_yes(...)`, `buy_no(...)`, `sell_no(...)`
- `cancel(order_id)`
- `log(...)`

Stored websocket datasets can also be replayed through the same engine with `MarketDataReplay`.

## 10B. Replay Backtests On Captured Datasets

Use `ReplayBacktester` when you want strategies to react directly to replayed `orderbook`, `ticker`, and `trade` events from stored datasets.

```python
from mykalshi.research import KalshiStrategy, ReplayBacktester

class BuyFirstQuoteStrategy(KalshiStrategy):
    def __init__(self):
        self.submitted = False

    def on_orderbook(self, context, event):
        if self.submitted:
            return
        context.buy_yes(event.market_ticker, quantity=1)
        self.submitted = True

result = ReplayBacktester().run_on_captured_dataset(
    BuyFirstQuoteStrategy(),
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
    initial_cash_cents=10000,
)
print(result.summary())
```

If you already loaded a merged replay timeline with `load_replay_event_stream(...)`, you can run it directly:

```python
from mykalshi.research import ReplayBacktester, load_replay_event_stream

timeline = load_replay_event_stream(
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
)

result = ReplayBacktester().run_on_replay_event_stream(
    timeline,
    BuyFirstQuoteStrategy(),
    initial_cash_cents=10000,
)
print(result.summary())
```

Replay backtests now enrich missing expiration and settlement events from Kalshi market metadata by default. That is what you want for real captured datasets, but if you are testing with synthetic tickers, disable it explicitly:

```python
result = ReplayBacktester().run_on_replay_event_stream(
    timeline,
    BuyFirstQuoteStrategy(),
    enrich_market_lifecycle=False,
    initial_cash_cents=10000,
)
```

The replay result object also exposes richer analytics and export helpers:

```python
market_view = result.market_summary("KXELONMARS-99")
position_view = result.position("KXELONMARS-99")

print(market_view.summary())
print(position_view.summary())

frames = result.to_dataframes()
print(frames["fills"].head())
print(frames["markets"].head())
```

If you want one higher-level object for the whole load-and-run step, use `ResearchSession` plus `ReplayDataset`:

```python
from mykalshi.research import ResearchSession

session = ResearchSession()
dataset = session.load_replay_dataset(
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
)
print(dataset.summary())

result = dataset.backtest(
    BuyFirstQuoteStrategy(),
    initial_cash_cents=10000,
)
print(result.summary())
```

Or run the same path in one call:

```python
result = session.run_replay_backtest(
    BuyFirstQuoteStrategy(),
    market_data_source="data/market-data.sqlite",
    orderbook_source="data/orderbook.sqlite",
    market_ticker="KXELONMARS-99",
    initial_cash_cents=10000,
)
print(result.summary())
```

## 11. Auto-Route Live And Historical Trades

```python
from mykalshi import routing

result = routing.get_trades_auto("KXELONMARS-99", start_ts="03/14/2026 00:00:00")
print(result["sources_used"])
print(result["total_count"])
```

## 12. Trading

Trading/account calls live under `mykalshi.trading`.

Examples:

```python
from mykalshi import trading

print(trading.get_account_limits())
print(trading.get_balance())
print(trading.get_orders(limit=10))
```

Because your current config is production, do not place/cancel/amend orders unless that is intentional.

## 13. What Broke In Your Terminal

### `historical.get_historical_trades(ticker="YOUR_TICKER", ...)`

That failed because `"YOUR_TICKER"` was a literal placeholder string, not a real archived market ticker.

### `capture_orderbook_sync("YOUR_TICKER", max_events=10, ...)`

That call did not fail because the websocket client is broken. It waited because:

- the placeholder ticker is not meaningful
- `max_events=10` tells it to keep waiting after the initial snapshot
- you interrupted it with `Ctrl+C`, which produced the `KeyboardInterrupt`

For a smoke test, use a real market ticker and `max_events=1`.

### `TradeBacktester().run_on_historical_trades("YOUR_TICKER", ...)`

Same issue as historical trades above: the placeholder ticker was not real.

## 14. Current Limits

This repo is usable, but not yet polished into a single “app” with one command or one end-to-end workflow.

Missing pieces still include:

- more websocket channels beyond orderbook-first capture
- a CLI
