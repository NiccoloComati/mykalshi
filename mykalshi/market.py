from __future__ import annotations

import json
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .fixed_point import dollars_to_cents
from .formatting import format_timestamp, parse_timestamp
from .orderbook import extract_orderbook_levels
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
    response = kalshi_get(f"/markets/{ticker}/orderbook", params, authenticated=True)
    if "orderbook" in response:
        return response

    yes_levels, no_levels = extract_orderbook_levels(response)
    response = dict(response)
    response["orderbook"] = {
        "yes": [[price_cents, float(size)] for price_cents, size in sorted(yes_levels.items())],
        "no": [[price_cents, float(size)] for price_cents, size in sorted(no_levels.items())],
    }
    return response


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


def get_all_markets(
    status=None,
    batch_size=1000,
    max_items=None,
    *,
    event_ticker=None,
    series_ticker=None,
    min_created_ts=None,
    max_created_ts=None,
    min_updated_ts=None,
    max_close_ts=None,
    min_close_ts=None,
    min_settled_ts=None,
    max_settled_ts=None,
    tickers=None,
    mve_filter=None,
):
    return collect_cursor_pages(
        lambda cursor: get_markets(
            limit=batch_size,
            cursor=cursor,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
            min_created_ts=min_created_ts,
            max_created_ts=max_created_ts,
            min_updated_ts=min_updated_ts,
            max_close_ts=max_close_ts,
            min_close_ts=min_close_ts,
            min_settled_ts=min_settled_ts,
            max_settled_ts=max_settled_ts,
            status=status,
            tickers=tickers,
            mve_filter=mve_filter,
        ),
        item_key="markets",
        max_items=max_items,
    )


def default_market_snapshot_anchor_path(snapshot_path):
    path = Path(snapshot_path)
    return path.with_suffix(path.suffix + ".anchor.json")


def _snapshot_timestamp_now():
    return datetime.now(timezone.utc)


def _snapshot_timestamp_from_name(snapshot_path):
    path = Path(snapshot_path)
    prefix = "all_markets_"
    if path.stem.startswith(prefix):
        suffix = path.stem[len(prefix):]
        try:
            return datetime.strptime(suffix, "%Y-%m-%d-%H-%M-%S").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def _read_snapshot_anchor(anchor_path):
    path = Path(anchor_path)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_snapshot_anchor(anchor_path, payload):
    path = Path(anchor_path)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def sync_market_snapshot_csv(
    snapshot_path,
    *,
    anchor_path=None,
    status=None,
    batch_size=1000,
    max_items=None,
):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for sync_market_snapshot_csv") from exc

    snapshot_file = Path(snapshot_path)
    if not snapshot_file.is_absolute():
        snapshot_file = Path.cwd() / snapshot_file
    snapshot_file.parent.mkdir(parents=True, exist_ok=True)

    anchor_file = default_market_snapshot_anchor_path(snapshot_file) if anchor_path is None else Path(anchor_path)
    if not anchor_file.is_absolute():
        anchor_file = Path.cwd() / anchor_file
    anchor_file.parent.mkdir(parents=True, exist_ok=True)

    snapshot_exists = snapshot_file.exists()
    refresh_started_at = _snapshot_timestamp_now()

    if not snapshot_exists:
        markets = get_all_markets(status=status, batch_size=batch_size, max_items=max_items)
        frame = pd.json_normalize(markets)
        frame.to_csv(snapshot_file, index=False)
        anchor_payload = {
            "snapshot_path": str(snapshot_file),
            "status": status,
            "mode": "full_refresh",
            "snapshot_created_at": refresh_started_at.isoformat(),
            "snapshot_cursor_ts": int(refresh_started_at.timestamp()),
            "market_count": int(len(frame)),
        }
        _write_snapshot_anchor(anchor_file, anchor_payload)
        return {
            "snapshot_path": snapshot_file,
            "anchor_path": anchor_file,
            "mode": "full_refresh",
            "market_count": int(len(frame)),
            "delta_count": int(len(frame)),
            "anchor": anchor_payload,
        }

    anchor_payload = _read_snapshot_anchor(anchor_file)
    if anchor_payload is not None:
        anchor_dt = datetime.fromisoformat(anchor_payload["snapshot_created_at"])
        mode = "incremental_refresh"
    else:
        anchor_dt = _snapshot_timestamp_from_name(snapshot_file)
        mode = "incremental_refresh_bootstrap_anchor"

    existing_frame = pd.read_csv(snapshot_file, low_memory=False)
    delta_markets = get_all_markets(
        status=status,
        batch_size=batch_size,
        max_items=max_items,
        min_updated_ts=int(anchor_dt.timestamp()),
    )

    if delta_markets:
        delta_frame = pd.json_normalize(delta_markets)
        if "ticker" not in existing_frame.columns or "ticker" not in delta_frame.columns:
            raise ValueError("Market snapshot sync requires a 'ticker' column in both snapshot and delta data")
        existing_frame = existing_frame.drop_duplicates(subset=["ticker"], keep="last")
        delta_frame = delta_frame.drop_duplicates(subset=["ticker"], keep="last")
        existing_frame = existing_frame.set_index("ticker", drop=False)
        delta_frame = delta_frame.set_index("ticker", drop=False)
        merged = pd.concat(
            [existing_frame.loc[~existing_frame.index.isin(delta_frame.index)], delta_frame],
            axis=0,
        )
        merged = merged.sort_index()
        merged.to_csv(snapshot_file, index=False)
        market_count = int(len(merged))
    else:
        market_count = int(len(existing_frame))

    new_anchor = {
        "snapshot_path": str(snapshot_file),
        "status": status,
        "mode": mode,
        "snapshot_created_at": refresh_started_at.isoformat(),
        "snapshot_cursor_ts": int(refresh_started_at.timestamp()),
        "market_count": market_count,
    }
    _write_snapshot_anchor(anchor_file, new_anchor)
    return {
        "snapshot_path": snapshot_file,
        "anchor_path": anchor_file,
        "mode": mode,
        "market_count": market_count,
        "delta_count": int(len(delta_markets)),
        "anchor": new_anchor,
    }


def build_candlestick(candlestick_data):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for build_candlestick") from exc

    records = []
    for entry in candlestick_data["candlesticks"]:
        price = entry.get("price", {})
        yes_bid = entry.get("yes_bid", {})
        records.append(
            {
                "Date": datetime.fromtimestamp(entry["end_period_ts"]),
                "Open": _extract_candlestick_price(price, "open") or _extract_candlestick_price(yes_bid, "open"),
                "High": _extract_candlestick_price(price, "high") or _extract_candlestick_price(yes_bid, "high"),
                "Low": _extract_candlestick_price(price, "low") or _extract_candlestick_price(yes_bid, "low"),
                "Close": _extract_candlestick_price(price, "close") or _extract_candlestick_price(yes_bid, "close"),
                "Volume": _extract_fixed_point_count(entry, "volume"),
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
            "volume": _extract_fixed_point_count(candle, "volume"),
            "open_interest": _extract_fixed_point_count(candle, "open_interest"),
        }
        for section in ["yes_bid", "yes_ask", "price"]:
            for key, value in candle.get(section, {}).items():
                normalized_key, normalized_value = _normalize_candlestick_field(key, value)
                row[f"{section}_{normalized_key}"] = normalized_value
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
        start_ts = datetime.fromtimestamp(parse_timestamp(start_ts))
    if isinstance(end_ts, str):
        end_ts = datetime.fromtimestamp(parse_timestamp(end_ts))

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


def _extract_fixed_point_count(payload, key):
    if payload.get(key) is not None:
        return float(payload[key])
    fixed_point_key = f"{key}_fp"
    if payload.get(fixed_point_key) is not None:
        return float(payload[fixed_point_key])
    return None


def _normalize_candlestick_field(key, value):
    if key.endswith("_dollars") and value is not None:
        return key.removesuffix("_dollars"), dollars_to_cents(value)
    return key, value


def _extract_candlestick_price(section, key):
    if section.get(key) is not None:
        return float(section[key])
    dollars_key = f"{key}_dollars"
    if section.get(dollars_key) is not None:
        return float(dollars_to_cents(section[dollars_key]))
    return None


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
