from __future__ import annotations

import random
import threading
import time
from datetime import datetime, timedelta

from .formatting import format_timestamp, parse_timestamp
from .transport import collect_cursor_pages, kalshi_get


def get_market(ticker):
    return kalshi_get(f"/markets/{ticker}")


def get_markets(
    limit=100,
    cursor=None,
    event_ticker=None,
    series_ticker=None,
    min_created_ts=None,
    max_created_ts=None,
    min_updated_ts=None,
    max_close_ts=None,
    min_close_ts=None,
    min_settled_ts=None,
    max_settled_ts=None,
    status=None,
    tickers=None,
    mve_filter=None,
):
    params = {
        "limit": limit,
        "cursor": cursor,
        "event_ticker": event_ticker,
        "series_ticker": series_ticker,
        "min_created_ts": parse_timestamp(min_created_ts) if min_created_ts else None,
        "max_created_ts": parse_timestamp(max_created_ts) if max_created_ts else None,
        "min_updated_ts": parse_timestamp(min_updated_ts) if min_updated_ts else None,
        "max_close_ts": parse_timestamp(max_close_ts) if max_close_ts else None,
        "min_close_ts": parse_timestamp(min_close_ts) if min_close_ts else None,
        "min_settled_ts": parse_timestamp(min_settled_ts) if min_settled_ts else None,
        "max_settled_ts": parse_timestamp(max_settled_ts) if max_settled_ts else None,
        "status": status,
        "tickers": tickers,
        "mve_filter": mve_filter,
    }
    return kalshi_get("/markets", {k: v for k, v in params.items() if v is not None})


def get_market_orderbook(ticker, depth=None):
    params = {"depth": depth} if depth is not None else None
    return kalshi_get(f"/markets/{ticker}/orderbook", params, authenticated=True)


def get_market_candlesticks(series_ticker, ticker, start_ts, end_ts, period_interval):
    params = {
        "start_ts": parse_timestamp(start_ts),
        "end_ts": parse_timestamp(end_ts),
        "period_interval": period_interval,
    }
    return kalshi_get(f"/series/{series_ticker}/markets/{ticker}/candlesticks", params)


def batch_get_market_candlesticks(market_tickers, start_ts, end_ts, period_interval, include_latest_before_start=False):
    tickers = market_tickers if isinstance(market_tickers, str) else ",".join(market_tickers)
    params = {
        "market_tickers": tickers,
        "start_ts": parse_timestamp(start_ts),
        "end_ts": parse_timestamp(end_ts),
        "period_interval": period_interval,
        "include_latest_before_start": include_latest_before_start,
    }
    return kalshi_get("/markets/candlesticks", params)


def get_trades(ticker=None, limit=100, cursor=None, min_ts=None, max_ts=None):
    params = {
        "ticker": ticker,
        "limit": limit,
        "cursor": cursor,
        "min_ts": parse_timestamp(min_ts) if min_ts else None,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
    }
    return kalshi_get("/markets/trades", {k: v for k, v in params.items() if v is not None})


def get_all_markets(status=None, batch_size=1000):
    return collect_cursor_pages(
        lambda cursor: get_markets(limit=batch_size, status=status, cursor=cursor),
        item_key="markets",
    )


def build_candlestick(candlestick_data):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for build_candlestick") from exc

    records = []
    for entry in candlestick_data["candlesticks"]:
        records.append(
            {
                "Date": datetime.fromtimestamp(entry["end_period_ts"]),
                "Open": entry["price"]["open"] or entry["yes_bid"].get("open"),
                "High": entry["price"]["high"] or entry["yes_bid"].get("high"),
                "Low": entry["price"]["low"] or entry["yes_bid"].get("low"),
                "Close": entry["price"]["close"] or entry["yes_bid"].get("close"),
                "Volume": entry["volume"],
            }
        )
    dataframe = pd.DataFrame(records)
    dataframe.set_index("Date", inplace=True)
    return dataframe


def candlesticks_to_df(candlestick_response):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for candlesticks_to_df") from exc

    rows = []
    for candle in candlestick_response["candlesticks"]:
        row = {
            "end_period": format_timestamp(candle["end_period_ts"]),
            "volume": candle["volume"],
            "open_interest": candle["open_interest"],
        }
        for section in ["yes_bid", "yes_ask", "price"]:
            for key, value in candle.get(section, {}).items():
                row[f"{section}_{key}"] = value
        rows.append(row)

    return pd.DataFrame(rows)


def get_full_market(series_ticker, ticker, period_interval, start_ts=None, end_ts=None):
    if isinstance(period_interval, str):
        period_interval = {"m": 1, "h": 60, "d": 1440}[period_interval.lower()]

    if start_ts is None or end_ts is None:
        market_meta = get_market(ticker)
        if start_ts is None:
            start_ts = datetime.fromisoformat(market_meta["market"]["open_time"].replace("Z", "")).replace(tzinfo=None)
        if end_ts is None:
            end_ts = datetime.fromisoformat(market_meta["market"]["close_time"].replace("Z", "")).replace(tzinfo=None)

    if isinstance(start_ts, str):
        start_ts = datetime.strptime(start_ts, "%m/%d/%Y")
    if isinstance(end_ts, str):
        end_ts = datetime.strptime(end_ts, "%m/%d/%Y")

    all_candles = []
    chunk = timedelta(minutes=period_interval * 5000)
    current_start = start_ts

    while current_start < end_ts:
        current_end = min(current_start + chunk, end_ts)
        response = get_market_candlesticks(
            series_ticker=series_ticker,
            ticker=ticker,
            start_ts=current_start.strftime("%m/%d/%Y %H:%M:%S"),
            end_ts=current_end.strftime("%m/%d/%Y %H:%M:%S"),
            period_interval=period_interval,
        )
        all_candles.extend(response.get("candlesticks", []))
        current_start = current_end

    return {"ticker": ticker, "candlesticks": all_candles}


def get_all_trades(ticker=None, min_ts=None, max_ts=None, batch_size=100, calls_per_sec=30):
    min_interval = 1.0 / calls_per_sec
    lock = threading.Lock()
    last_call = 0.0

    def wait_rate_limit():
        nonlocal last_call
        with lock:
            now = time.time()
            elapsed = now - last_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            last_call = time.time()

    all_trades = []
    cursor = None
    while True:
        wait_rate_limit()
        response = get_trades(
            ticker=ticker,
            limit=batch_size,
            cursor=cursor,
            min_ts=min_ts,
            max_ts=max_ts,
        )
        trades = response.get("trades", [])
        all_trades.extend(trades)
        cursor = response.get("cursor")
        if not cursor or len(trades) < batch_size:
            break

    return {"ticker": ticker, "trades": all_trades, "total_count": len(all_trades)}


def get_all_trades_robust(
    ticker=None,
    min_ts=None,
    max_ts=None,
    batch_size=100,
    calls_per_sec=30,
    max_retries=5,
    base_backoff=0.1,
):
    min_interval = 1.0 / calls_per_sec
    lock = threading.Lock()
    last_call = 0.0

    def wait_rate_limit():
        nonlocal last_call
        with lock:
            now = time.time()
            elapsed = now - last_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            last_call = time.time()

    def make_request_with_retry(cursor=None):
        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                wait_rate_limit()
                return get_trades(
                    ticker=ticker,
                    limit=batch_size,
                    cursor=cursor,
                    min_ts=min_ts,
                    max_ts=max_ts,
                )
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries:
                    delay = base_backoff * (2 ** (attempt - 1)) * random.uniform(0.8, 1.2)
                    time.sleep(delay)
        raise last_exc

    all_trades = []
    cursor = None
    while True:
        try:
            response = make_request_with_retry(cursor)
        except Exception as exc:
            print(f"Error fetching trades for {ticker}: {exc}")
            break

        trades = response.get("trades", [])
        all_trades.extend(trades)
        cursor = response.get("cursor")
        if not cursor or len(trades) < batch_size:
            break

    return {"ticker": ticker, "trades": all_trades, "total_count": len(all_trades)}


def trades_to_dataframe(trades_result):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for trades_to_dataframe") from exc

    if not trades_result.get("trades"):
        return pd.DataFrame()

    rows = []
    for trade in trades_result["trades"]:
        rows.append(
            {
                "ticker": trade.get("ticker"),
                "timestamp": format_timestamp(trade.get("ts")) if trade.get("ts") else None,
                "ts": trade.get("ts"),
                "price": trade.get("price"),
                "size": trade.get("size"),
                "side": trade.get("side"),
                "order_id": trade.get("order_id"),
                "trade_id": trade.get("trade_id"),
            }
        )

    dataframe = pd.DataFrame(rows)
    if not dataframe.empty and "ts" in dataframe.columns:
        dataframe["datetime"] = pd.to_datetime(dataframe["ts"], unit="s")
        dataframe = dataframe.sort_values("ts")
    return dataframe
