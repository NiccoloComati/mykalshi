from __future__ import annotations

from .formatting import parse_timestamp
from .transport import kalshi_delete, kalshi_get, kalshi_post


def get_balance():
    return kalshi_get("/portfolio/balance", authenticated=True)


def get_fills(ticker=None, order_id=None, min_ts=None, max_ts=None, limit=100, cursor=None):
    params = {
        "ticker": ticker,
        "order_id": order_id,
        "min_ts": parse_timestamp(min_ts) if min_ts else None,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
        "limit": limit,
        "cursor": cursor,
    }
    return kalshi_get("/portfolio/fills", {k: v for k, v in params.items() if v is not None}, authenticated=True)


def get_orders(ticker=None, event_ticker=None, min_ts=None, max_ts=None, status=None, limit=100, cursor=None):
    params = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "min_ts": parse_timestamp(min_ts) if min_ts else None,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
        "status": status,
        "limit": limit,
        "cursor": cursor,
    }
    return kalshi_get("/portfolio/orders", {k: v for k, v in params.items() if v is not None}, authenticated=True)


def get_order(order_id):
    return kalshi_get(f"/portfolio/orders/{order_id}", authenticated=True)


def create_order(order_data):
    return kalshi_post("/portfolio/orders", order_data, authenticated=True)


def batch_create_orders(order_list):
    return kalshi_post("/portfolio/orders/batched", {"orders": order_list}, authenticated=True)


def cancel_order(order_id):
    return kalshi_delete(f"/portfolio/orders/{order_id}", authenticated=True)


def batch_cancel_orders(order_ids):
    return kalshi_delete("/portfolio/orders/batched", body={"ids": order_ids}, authenticated=True)


def amend_order(order_id, *, price=None, count=None):
    body = {k: v for k, v in {"price": price, "count": count}.items() if v is not None}
    return kalshi_post(f"/portfolio/orders/{order_id}/amend", body, authenticated=True)


def decrease_order(order_id, reduce_by=None, reduce_to=None):
    body = {"reduce_by": reduce_by, "reduce_to": reduce_to}
    return kalshi_post(
        f"/portfolio/orders/{order_id}/decrease",
        {k: v for k, v in body.items() if v is not None},
        authenticated=True,
    )


def get_order_queue_positions(market_tickers=None, event_ticker=None, subaccount=None):
    params = {
        "market_tickers": market_tickers,
        "event_ticker": event_ticker,
        "subaccount": subaccount,
    }
    return kalshi_get(
        "/portfolio/orders/queue_positions",
        {k: v for k, v in params.items() if v is not None},
        authenticated=True,
    )


def get_order_queue_position(order_id):
    return kalshi_get(f"/portfolio/orders/{order_id}/queue_position", authenticated=True)


def get_positions(ticker=None, event_ticker=None, count_filter=None, settlement_status="unsettled", limit=100, cursor=None):
    params = {
        "ticker": ticker,
        "event_ticker": event_ticker,
        "count_filter": count_filter,
        "settlement_status": settlement_status,
        "limit": limit,
        "cursor": cursor,
    }
    return kalshi_get("/portfolio/positions", {k: v for k, v in params.items() if v is not None}, authenticated=True)


def get_portfolio_settlements(limit=100, min_ts=None, max_ts=None, cursor=None):
    params = {
        "limit": limit,
        "min_ts": parse_timestamp(min_ts) if min_ts else None,
        "max_ts": parse_timestamp(max_ts) if max_ts else None,
        "cursor": cursor,
    }
    return kalshi_get("/portfolio/settlements", {k: v for k, v in params.items() if v is not None}, authenticated=True)


def get_total_resting_order_value():
    return kalshi_get("/portfolio/summary/total_resting_order_value", authenticated=True)
