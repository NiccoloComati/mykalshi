import pandas as pd
from datetime import datetime, timedelta
import time
import threading
import random
from requests.exceptions import HTTPError
from .transport import kalshi_get
from .formatting import parse_timestamp, format_timestamp

def get_market(ticker):
    """
    Get market details by market ticker.

    Parameters:
        ticker (str): Market ticker
    """
    return kalshi_get(f"/markets/{ticker}")

def get_markets(limit=100, cursor=None, event_ticker=None, series_ticker=None,
                max_close_ts=None, min_close_ts=None, status=None, tickers=None):
    """
    Get a list of markets with optional filters.

    Parameters:
        limit (int): Max number of results (1-1000)
        cursor (str): Used for pagination
        event_ticker (str): Filter markets belonging to this event
        series_ticker (str): Filter markets belonging to this series
        max_close_ts (int): Markets closing before this Unix timestamp
        min_close_ts (int): Markets closing after this Unix timestamp
        status (str): Filter by market status: unopened, open, closed, settled
        tickers (str): Comma-separated tickers to filter
    """
    max_close_ts = parse_timestamp(max_close_ts) if max_close_ts else None
    min_close_ts = parse_timestamp(min_close_ts) if min_close_ts else None
    params = {
        "limit": limit,
        "cursor": cursor,
        "event_ticker": event_ticker,
        "series_ticker": series_ticker,
        "max_close_ts": max_close_ts,
        "min_close_ts": min_close_ts,
        "status": status,
        "tickers": tickers
    }
    return kalshi_get("/markets", {k: v for k, v in params.items() if v is not None})

def get_market_orderbook(ticker, depth=None):
    """
    Retrieve the order book for a given market.

    Parameters:
        ticker (str): Market ticker
        depth (int): Max number of levels per side (optional)
    """
    params = {"depth": depth} if depth else None
    return kalshi_get(f"/markets/{ticker}/orderbook", params)

def get_market_candlesticks(series_ticker, ticker, start_ts, end_ts, period_interval):
    """
    Retrieve historical candlestick data for a market.

    Parameters:
        series_ticker (str): The series the market belongs to
        ticker (str): The market ticker
        start_ts (int): Unix timestamp for beginning of range
        end_ts (int): Unix timestamp for end of range
        period_interval (int): Time per candle in minutes (1, 60, or 1440)
    """
    start_ts = parse_timestamp(start_ts)
    end_ts = parse_timestamp(end_ts)
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "period_interval": period_interval
    }
    return kalshi_get(f"/series/{series_ticker}/markets/{ticker}/candlesticks", params)

def get_trades(ticker=None, limit=100, cursor=None, min_ts=None, max_ts=None):
    """
    Get trade history for one or more markets.

    Parameters:
        ticker (str): Specific market ticker
        limit (int): Max number of trades to retrieve
        cursor (str): Pagination cursor
        min_ts (int): Minimum timestamp (Unix time)
        max_ts (int): Maximum timestamp (Unix time)
    """
    min_ts = parse_timestamp(min_ts) if min_ts else None
    max_ts = parse_timestamp(max_ts) if max_ts else None
    params = {
        "ticker": ticker,
        "limit": limit,
        "cursor": cursor,
        "min_ts": min_ts,
        "max_ts": max_ts
    }
    return kalshi_get("/markets/trades", {k: v for k, v in params.items() if v is not None})

def get_all_markets(status=None, batch_size=1000):
    all_markets = []
    cursor = None
    while True:
        response = get_markets(limit=batch_size, status=status, cursor=cursor)
        all_markets.extend(response["markets"])
        
        cursor = response.get("cursor")  # <==== this is the real fix
        if not cursor:
            break

    return all_markets

def build_candlestick(candlestick_data):
    """Convert Kalshi candlestick API data to DataFrame for plotting."""
    records = []
    for entry in candlestick_data["candlesticks"]:
        row = {
            "Date": datetime.fromtimestamp(entry["end_period_ts"]),
            "Open": entry["price"]["open"] or entry["yes_bid"].get("open"),
            "High": entry["price"]["high"] or entry["yes_bid"].get("high"),
            "Low": entry["price"]["low"] or entry["yes_bid"].get("low"),
            "Close": entry["price"]["close"] or entry["yes_bid"].get("close"),
            "Volume": entry["volume"]
        }
        records.append(row)
    df = pd.DataFrame(records)
    df.set_index("Date", inplace=True)
    return df

def candlesticks_to_df(candlestick_response):
    """Convert candlestick API response to DataFrame with readable timestamps."""
    rows = []
    for candle in candlestick_response["candlesticks"]:
        row = {
            "end_period": format_timestamp(candle["end_period_ts"]),
            "volume": candle["volume"],
            "open_interest": candle["open_interest"],
        }

        # Flatten nested dictionaries
        for section in ["yes_bid", "yes_ask", "price"]:
            for k, v in candle.get(section, {}).items():
                row[f"{section}_{k}"] = v

        rows.append(row)

    return pd.DataFrame(rows)

def get_full_market(series_ticker, ticker, period_interval, start_ts=None, end_ts=None):
    """
    Fetch full historical candlesticks for a market by looping in 5000-period chunks.

    Args:
        series_ticker (str)
        ticker (str)
        start_ts (str or datetime): MM/DD/YYYY or datetime object
        end_ts (str or datetime): MM/DD/YYYY or datetime object
        period_interval (int): Minutes (1, 60, 1440)

    Returns:
        List of all candlestick entries
    """
    if isinstance(period_interval, str):
        period_interval = {"m": 1, "h": 60, "d": 1440}[period_interval.lower()]

    if start_ts is None or end_ts is None:
        market_meta = get_market(ticker)
        if start_ts is None:
            start_ts = datetime.fromisoformat(market_meta['market']['open_time'].replace("Z", "")).replace(tzinfo=None)
        if end_ts is None:
            end_ts = datetime.fromisoformat(market_meta['market']['close_time'].replace("Z", "")).replace(tzinfo=None)

    if isinstance(start_ts, str):
        start_ts = datetime.strptime(start_ts, "%m/%d/%Y")
    if isinstance(end_ts, str):
        end_ts = datetime.strptime(end_ts, "%m/%d/%Y")

    all_candles = []
    chunk = timedelta(minutes=period_interval * 5000)
    cur_start = start_ts

    while cur_start < end_ts:
        cur_end = min(cur_start + chunk, end_ts)
        response = get_market_candlesticks(
            series_ticker=series_ticker,
            ticker=ticker,
            start_ts=cur_start.strftime("%m/%d/%Y %H:%M:%S"),
            end_ts=cur_end.strftime("%m/%d/%Y %H:%M:%S"),
            period_interval=period_interval
        )
        all_candles.extend(response.get("candlesticks", []))
        cur_start = cur_end

    return {
        "ticker": ticker,
        "candlesticks": all_candles
    }

def get_all_trades(ticker=None, min_ts=None, max_ts=None, batch_size=100, calls_per_sec=30):
    """
    Get all trade history for one or more markets using pagination and rate limiting.
    
    Parameters:
        ticker (str): Specific market ticker
        min_ts (int): Minimum timestamp (Unix time)
        max_ts (int): Maximum timestamp (Unix time)
        batch_size (int): Number of trades to fetch per request (max 100)
        calls_per_sec (int): Maximum API calls per second for rate limiting
    
    Returns:
        List of all trade entries
    """
    # Rate limiting setup
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
            max_ts=max_ts
        )
        
        trades = response.get("trades", [])
        all_trades.extend(trades)
        
        # Check for more pages
        cursor = response.get("cursor")
        if not cursor or len(trades) < batch_size:
            break
    
    return {
        "ticker": ticker,
        "trades": all_trades,
        "total_count": len(all_trades)
    }

def get_all_trades_robust(ticker=None, min_ts=None, max_ts=None, batch_size=100, 
                          calls_per_sec=30, max_retries=5, base_backoff=0.1):
    """
    Get all trade history for one or more markets using pagination, rate limiting, and error handling.
    
    Parameters:
        ticker (str): Specific market ticker
        min_ts (int): Minimum timestamp (Unix time)
        max_ts (int): Maximum timestamp (Unix time)
        batch_size (int): Number of trades to fetch per request (max 100)
        calls_per_sec (int): Maximum API calls per second for rate limiting
        max_retries (int): Maximum number of retries for failed requests
        base_backoff (float): Base delay for exponential backoff
    
    Returns:
        List of all trade entries
    """
    # Rate limiting setup
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
                    max_ts=max_ts
                )
            except HTTPError as http_err:
                code = getattr(http_err.response, "status_code", None)
                if code == 429 and attempt < max_retries:
                    delay = base_backoff * (2 ** (attempt - 1)) * random.uniform(0.8, 1.2)
                    time.sleep(delay)
                    last_exc = http_err
                    continue
                last_exc = http_err
                break
            except Exception as exc:
                if attempt < max_retries:
                    delay = base_backoff * random.uniform(0.5, 1.5)
                    time.sleep(delay)
                    last_exc = exc
                    continue
                last_exc = exc
                break
        
        # If we get here, all retries failed
        raise last_exc
    
    all_trades = []
    cursor = None
    
    while True:
        try:
            response = make_request_with_retry(cursor)
            
            trades = response.get("trades", [])
            all_trades.extend(trades)
            
            # Check for more pages
            cursor = response.get("cursor")
            if not cursor or len(trades) < batch_size:
                break
                
        except Exception as e:
            print(f"Error fetching trades for {ticker}: {e}")
            break
    
    return {
        "ticker": ticker,
        "trades": all_trades,
        "total_count": len(all_trades)
    }

def trades_to_dataframe(trades_result):
    """
    Convert trades result to a pandas DataFrame for easier analysis.
    
    Parameters:
        trades_result (dict): Result from get_all_trades or get_all_trades_robust
    
    Returns:
        pandas.DataFrame: DataFrame with trade data
    """
    if not trades_result.get('trades'):
        return pd.DataFrame()
    
    rows = []
    for trade in trades_result['trades']:
        row = {
            'ticker': trade.get('ticker'),
            'timestamp': format_timestamp(trade.get('ts')) if trade.get('ts') else None,
            'ts': trade.get('ts'),
            'price': trade.get('price'),
            'size': trade.get('size'),
            'side': trade.get('side'),
            'order_id': trade.get('order_id'),
            'trade_id': trade.get('trade_id')
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    if not df.empty:
        # Convert timestamp to datetime if available
        if 'ts' in df.columns:
            df['datetime'] = pd.to_datetime(df['ts'], unit='s')
        
        # Sort by timestamp
        if 'ts' in df.columns:
            df = df.sort_values('ts')
    
    return df
