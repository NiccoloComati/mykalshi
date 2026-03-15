# Usage Guide

This repo is currently a Python toolkit, not a standalone app.

The main workflows that are usable today are:

- live market discovery and data pulls
- archived historical data pulls
- websocket orderbook capture
- SQLite and Parquet dataset writing
- simple historical-trade backtests

## 1. Setup

Activate the local virtual environment and install editable dependencies:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -e .[analysis,storage,websocket]
```

Your root `.env` currently resolves to production, so authenticated calls hit the live account unless you switch `KALSHI_ENV=demo`.

## 2. Import Surface

The main modules are:

```python
from mykalshi import market, historical, events, trading
from mykalshi.research import (
    KalshiWebsocketClient,
    SQLiteOrderbookSink,
    ParquetOrderbookSink,
    MultiOrderbookSink,
    TradeBacktester,
    TradeSignal,
)
```

## 3. Live Market Discovery

Do not use literal placeholder strings like `"YOUR_TICKER"`. Pull a real market first.

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

## 8. Backtesting On Archived Trades

Use a real archived ticker, not a placeholder.

```python
from mykalshi import historical
from mykalshi.research import TradeBacktester, TradeSignal

sample_trade = historical.get_historical_trades(limit=1)["trades"][0]
ticker = sample_trade["ticker"]

def strategy(context, trade):
    if context.yes_position == 0:
        return TradeSignal("buy_yes", quantity=1)
    return None

result = TradeBacktester().run_on_historical_trades(
    ticker,
    strategy,
    initial_cash_cents=10000,
)
print(result.summary())
```

## 9. Trading

Trading/account calls live under `mykalshi.trading`.

Examples:

```python
from mykalshi import trading

print(trading.get_balance())
print(trading.get_orders(limit=10))
```

Because your current config is production, do not place/cancel/amend orders unless that is intentional.

## 10. What Broke In Your Terminal

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

## 11. Current Limits

This repo is usable, but not yet polished into a single “app” with one command or one end-to-end workflow.

Missing pieces still include:

- replay helpers over stored datasets
- richer market-selection convenience helpers
- more websocket channels beyond orderbook-first capture
- more realistic fee/execution modeling in backtests
- a CLI
