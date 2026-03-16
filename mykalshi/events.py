from __future__ import annotations

from datetime import datetime

from .fixed_point import dollars_to_cents
from .transport import collect_cursor_pages, kalshi_get


def get_event(event_ticker, with_nested_markets=False):
    return kalshi_get(
        f"/events/{event_ticker}",
        {"with_nested_markets": with_nested_markets},
    )


def get_events(limit=100, cursor=None, status=None, series_ticker=None, with_nested_markets=False):
    params = {
        "limit": limit,
        "cursor": cursor,
        "status": status,
        "series_ticker": series_ticker,
        "with_nested_markets": with_nested_markets,
    }
    return kalshi_get("/events", {k: v for k, v in params.items() if v is not None})


def get_all_events(status=None, series_ticker=None, with_nested_markets=False, batch_size=200, max_items=None):
    return collect_cursor_pages(
        lambda cursor: get_events(
            cursor=cursor,
            status=status,
            series_ticker=series_ticker,
            with_nested_markets=with_nested_markets,
            limit=batch_size,
        ),
        item_key="events",
        max_items=max_items,
    )


def get_series_list(category=None, include_product_metadata=False, limit=100, cursor=None):
    params = {
        "category": category,
        "include_product_metadata": include_product_metadata,
        "limit": limit,
        "cursor": cursor,
    }
    return kalshi_get("/series/", {k: v for k, v in params.items() if v is not None})


def get_series(series_ticker):
    return kalshi_get(f"/series/{series_ticker}")


def get_all_series(category=None, include_product_metadata=False, batch_size=100, max_items=None):
    return collect_cursor_pages(
        lambda cursor: get_series_list(
            category=category,
            include_product_metadata=include_product_metadata,
            limit=batch_size,
            cursor=cursor,
        ),
        item_key="series",
        max_items=max_items,
    )


def get_event_collection(collection_ticker):
    return kalshi_get(f"/multivariate_event_collections/{collection_ticker}")


def get_event_collections(status=None, associated_event_ticker=None, series_ticker=None, limit=100, cursor=None):
    params = {
        "status": status,
        "associated_event_ticker": associated_event_ticker,
        "series_ticker": series_ticker,
        "limit": limit,
        "cursor": cursor,
    }
    return kalshi_get("/multivariate_event_collections/", {k: v for k, v in params.items() if v is not None})


def get_milestone(milestone_id):
    return kalshi_get(f"/milestones/{milestone_id}")


def get_milestones(limit, minimum_start_date=None, category=None, type=None, related_event_ticker=None, cursor=None):
    params = {
        "limit": limit,
        "minimum_start_date": minimum_start_date,
        "category": category,
        "type": type,
        "related_event_ticker": related_event_ticker,
        "cursor": cursor,
    }
    return kalshi_get("/milestones/", {k: v for k, v in params.items() if v is not None})


def event_info(event_ticker):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for event_info") from exc

    data = get_event(event_ticker)
    event = data["event"]
    markets = data["markets"]

    market_df = pd.DataFrame(
        [
            {
                "market_ticker": market["ticker"],
                "yes_sub_title": market.get("yes_sub_title"),
                "range": market.get("subtitle")
                or f"{market.get('floor_strike', '')} - {market.get('cap_strike', '')}",
                "strike_type": market.get("strike_type"),
                "last_price": _market_price_cents(market, "last_price", "last_price_dollars"),
                "yes_bid": _market_price_cents(market, "yes_bid", "yes_bid_dollars"),
                "yes_ask": _market_price_cents(market, "yes_ask", "yes_ask_dollars"),
                "no_bid": _market_price_cents(market, "no_bid", "no_bid_dollars"),
                "no_ask": _market_price_cents(market, "no_ask", "no_ask_dollars"),
                "volume": _market_count(market, "volume"),
                "open_time": market.get("open_time"),
                "close_time": market.get("close_time"),
                "status": market.get("status"),
                "rules_primary": market.get("rules_primary"),
            }
            for market in markets
        ]
    )

    if "strike_date" in event:
        event["strike_date"] = datetime.fromisoformat(event["strike_date"].replace("Z", "+00:00"))

    return {
        "event_info": {
            "event_ticker": event["event_ticker"],
            "series_ticker": event["series_ticker"],
            "title": event["title"],
            "subtitle": event.get("sub_title", ""),
            "strike_date": event.get("strike_date"),
            "category": event.get("category", ""),
            "market_count": len(market_df),
        },
        "markets": market_df,
    }


def _market_price_cents(payload, legacy_key, dollars_key):
    if payload.get(legacy_key) is not None:
        return payload[legacy_key]
    if payload.get(dollars_key) is not None:
        return dollars_to_cents(payload[dollars_key])
    return None


def _market_count(payload, legacy_key):
    if payload.get(legacy_key) is not None:
        return payload[legacy_key]
    fixed_point_key = f"{legacy_key}_fp"
    if payload.get(fixed_point_key) is not None:
        return float(payload[fixed_point_key])
    return None


def get_structured_target(structured_target_id):
    return kalshi_get(f"/structured_targets/{structured_target_id}")
