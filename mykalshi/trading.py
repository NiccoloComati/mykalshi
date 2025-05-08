from .transport import kalshi_get, kalshi_post, kalshi_delete
from .formatting import parse_timestamp

def get_balance():
    """Retrieve the current account balance."""
    return kalshi_get("/portfolio/balance")

def get_fills(ticker=None, order_id=None, min_ts=None, max_ts=None, limit=100, cursor=None):
    """Retrieve executed trade fills."""
    min_ts = parse_timestamp(min_ts) if min_ts else None
    max_ts = parse_timestamp(max_ts) if max_ts else None
    params = {
        "ticker": ticker,
        "order_id": order_id,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "limit": limit,
        "cursor": cursor
    }
    return kalshi_get("/portfolio/fills", {k: v for k, v in params.items() if v is not None})

def get_orders(ticker=None, event_ticker=None, min_ts=None, max_ts=None, status=None, limit=100, cursor=None):
    """Retrieve all orders."""
    min_ts = parse_timestamp(min_ts) if min_ts else None
    max_ts = parse_timestamp(max_ts) if max_ts else None
    params = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "status": status,
        "limit": limit,
        "cursor": cursor
    }
    return kalshi_get("/portfolio/orders", {k: v for k, v in params.items() if v is not None})

def get_order(order_id):
    """Retrieve a specific order by ID."""
    return kalshi_get(f"/portfolio/orders/{order_id}")

def create_order(order_data):
    """Submit a new order.

    Args:
        order_data (dict): Must include fields like ticker, side, type, count, and price (yes_price or no_price).

    Returns:
        Confirmation with order ID.
    """
    return kalshi_post("/portfolio/orders", order_data)

def batch_create_orders(order_list):
    """Submit multiple orders in a single request (advanced users only)."""
    return kalshi_post("/portfolio/orders/batched", {"orders": order_list})

def cancel_order(order_id):
    """Cancel a specific order by ID."""
    return kalshi_delete(f"/portfolio/orders/{order_id}")

def batch_cancel_orders(order_ids):
    """Cancel multiple orders by ID (advanced users only)."""
    return kalshi_delete("/portfolio/orders/batched", {"ids": order_ids})

def decrease_order(order_id, reduce_by=None, reduce_to=None):
    """Decrease the number of contracts in an existing order."""
    body = {"reduce_by": reduce_by, "reduce_to": reduce_to}
    return kalshi_post(f"/portfolio/orders/{order_id}/decrease", {k: v for k, v in body.items() if v is not None})

def get_positions(ticker=None, event_ticker=None, count_filter=None, settlement_status="unsettled", limit=100, cursor=None):
    """Get all market positions."""
    params = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "count_filter": count_filter,
        "settlement_status": settlement_status,
        "limit": limit,
        "cursor": cursor
    }
    return kalshi_get("/portfolio/positions", {k: v for k, v in params.items() if v is not None})

def get_portfolio_settlements(limit=100, min_ts=None, max_ts=None, cursor=None):
    """Retrieve historical portfolio settlements."""
    min_ts = parse_timestamp(min_ts) if min_ts else None
    max_ts = parse_timestamp(max_ts) if max_ts else None
    params = {
        "limit": limit,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "cursor": cursor
    }
    return kalshi_get("/portfolio/settlements", {k: v for k, v in params.items() if v is not None})

def get_total_resting_order_value():
    """Retrieve the total value of resting orders (FCM members only)."""
    return kalshi_get("/portfolio/summary/total_resting_order_value")
