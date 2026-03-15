from __future__ import annotations

from .formatting import parse_timestamp
from .transport import collect_cursor_pages, kalshi_get


def get_historical_cutoff():
    return kalshi_get("/historical/cutoff")


def get_historical_market(ticker):
    return kalshi_get(f"/historical/markets/{ticker}")


def get_historical_markets(limit=100, cursor=None, tickers=None, event_ticker=None, mve_filter=None):
    params = {
        "limit": limit,
        "cursor": cursor,
        "tickers": tickers,
        "event_ticker": event_ticker,
        "mve_filter": mve_filter,
    }
    return kalshi_get("/historical/markets", {k: v for k, v in params.items() if v is not None})


def get_all_historical_markets(tickers=None, event_ticker=None, mve_filter=None, batch_size=1000):
    return collect_cursor_pages(
        lambda cursor: get_historical_markets(
            limit=batch_size,
            cursor=cursor,
            tickers=tickers,
            event_ticker=event_ticker,
            mve_filter=mve_filter,
        ),
        item_key="markets",
    )


def get_historical_market_candlesticks(ticker, start_ts, end_ts, period_interval):
    params = {
        "start_ts": parse_timestamp(start_ts),
        "end_ts": parse_timestamp(end_ts),
        "period_interval": period_interval,
    }
    return kalshi_get(f"/historical/markets/{ticker}/candlesticks", params)


def get_historical_trades(ticker=None, limit=100, cursor=None, min_ts=None, max_ts=None):
    params = {
        "ticker": ticker,
        "limit": limit,
        "cursor": cursor,
        "min_ts": parse_timestamp(min_ts) if min_ts else None,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
    }
    return kalshi_get("/historical/trades", {k: v for k, v in params.items() if v is not None})


def get_all_historical_trades(ticker=None, min_ts=None, max_ts=None, batch_size=1000):
    return {
        "ticker": ticker,
        "trades": collect_cursor_pages(
            lambda cursor: get_historical_trades(
                ticker=ticker,
                limit=batch_size,
                cursor=cursor,
                min_ts=min_ts,
                max_ts=max_ts,
            ),
            item_key="trades",
        ),
    }


def get_historical_orders(ticker=None, limit=100, cursor=None, max_ts=None):
    params = {
        "ticker": ticker,
        "limit": limit,
        "cursor": cursor,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
    }
    return kalshi_get("/historical/orders", {k: v for k, v in params.items() if v is not None}, authenticated=True)


def get_historical_fills(ticker=None, limit=100, cursor=None, max_ts=None):
    params = {
        "ticker": ticker,
        "limit": limit,
        "cursor": cursor,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
    }
    return kalshi_get("/historical/fills", {k: v for k, v in params.items() if v is not None}, authenticated=True)
